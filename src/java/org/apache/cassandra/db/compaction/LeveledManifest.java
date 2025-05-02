/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.db.compaction;

import java.io.IOException;
import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.RowPosition;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.utils.Pair;

public class LeveledManifest
{
    private static final Logger logger = LoggerFactory.getLogger(LeveledManifest.class);

    /**
     * Cassandra's default value for the minimum number of sstables needed for STCS
     * in L0.
     */
    private static final int DEFAULT_MIN_COMPACTING_L0 = 2;

    /**
     * limit the number of L0 sstables we do at once, because compaction bloom filter creation
     * uses a pessimistic estimate of how many keys overlap (none), so we risk wasting memory
     * or even OOMing when compacting highly overlapping sstables
     */
    private static final int MAX_COMPACTING_L0 = 32;
    /**
     * If we go this many rounds without compacting
     * in the highest level, we start bringing in sstables from
     * that level into lower level compactions
     */
    private static final int NO_COMPACTION_LIMIT = 25;

    /**
     * Sets the minimum number of sstables needed in L0 to perform a STCS. It has to be between
     * DEFAULT_MIN_COMPACTING_L0 and MAX_COMPACTING_L0.
     * In Fauna, we flush often to keep the persisted timestamp moving forward so we always emit small sstables.
     * With Cassandra's default value, we end up spending all of our time in compactions
     * on taking a small newly flushed sstable and compacting it with an increasingly larger one,
     * up to maxSSTableSizeInMB. Using a slightly larger minimum cuts down our write amplification and resource
     * utilization for no downside.
     */
    private static final String MIN_COMPACTING_L0 = "fauna.min-l0-compacting";

    /**
     * Similar to the option above, except that it only considers the total number of files
     * that will be rewritten during the compaction.
     *
     * This is evaluated only if MAX_SPLASH_PERCENT_OPTION would allow the compaction.
     */
    private static final String MAX_SPLASH_FILE_OPTION = "fauna.max-splash-files";

    /**
     * When true, disables splash prevention by allowing compactions
     * to rewrite an arbitrary number of bytes and/or files.
     */
    private static final String ALLOW_SPLASH_OPTION = "fauna.allow-splash";

    private final ColumnFamilyStore cfs;
    @VisibleForTesting
    protected final List<SSTableReader>[] generations;
    private final RowPosition[] lastCompactedKeys;
    private final long maxSSTableSizeInBytes;
    private final int minL0Compacting;
    private final int maxSplashFiles;
    private final SizeTieredCompactionStrategyOptions options;
    private final int [] compactionCounter;

    public LeveledManifest(ColumnFamilyStore cfs, int maxSSTableSizeInMB, SizeTieredCompactionStrategyOptions options)
    {
        this.cfs = cfs;
        this.maxSSTableSizeInBytes = maxSSTableSizeInMB * 1024L * 1024L;
        this.options = options;

        // allocate enough generations for a PB of data, with a 1-MB sstable size.  (Note that if maxSSTableSize is
        // updated, we will still have sstables of the older, potentially smaller size.  So don't make this
        // dependent on maxSSTableSize.)
        int n = (int) Math.log10(1000 * 1000 * 1000);
        generations = new List[n];
        lastCompactedKeys = new RowPosition[n];
        for (int i = 0; i < generations.length; i++)
        {
            generations[i] = new ArrayList<>();
            // Start at a random token to encourage compaction through the token space.
            // It used to start at the beginning each run of the process, so the end of the
            // space wouldn't get compacted if restarts were frequent enough compared to
            // the amount and velocity of data.
            lastCompactedKeys[i] = cfs.partitioner.getRandomToken().minKeyBound();
        }
        compactionCounter = new int[n];

        String splashFilesOption = System.getProperty(MAX_SPLASH_FILE_OPTION, "30");
        int optFiles;
        try {
            optFiles = Integer.valueOf(splashFilesOption);

            // Clamp the input.
            optFiles = Math.min(optFiles, Integer.MAX_VALUE);
        } catch (NumberFormatException ex) {
            logger.warn("Invalid value for {}: {}. Disabling splash prevention.",
                    MAX_SPLASH_FILE_OPTION, splashFilesOption);
            optFiles = Integer.MAX_VALUE;
        }

        String minCompactingL0Option =
                System.getProperty(MIN_COMPACTING_L0, Integer.toString(DEFAULT_MIN_COMPACTING_L0));
        int minL0;
        try {
            minL0 = Integer.valueOf(minCompactingL0Option);

            // Value should be between C*'s default and the max number of L0 files.
            minL0 = Math.max(minL0, DEFAULT_MIN_COMPACTING_L0);
            minL0 = Math.min(minL0, MAX_COMPACTING_L0);
        } catch (NumberFormatException ex) {
            logger.warn("Invalid value for {}: {}. Setting to C*'s default of {}.",
                    MIN_COMPACTING_L0, minCompactingL0Option, DEFAULT_MIN_COMPACTING_L0);
            minL0 = DEFAULT_MIN_COMPACTING_L0;
        }

        this.maxSplashFiles = optFiles;
        this.minL0Compacting = minL0;
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, List<SSTableReader> sstables)
    {
        return create(cfs, maxSSTableSize, sstables, new SizeTieredCompactionStrategyOptions());
    }

    public static LeveledManifest create(ColumnFamilyStore cfs, int maxSSTableSize, Iterable<SSTableReader> sstables, SizeTieredCompactionStrategyOptions options)
    {
        LeveledManifest manifest = new LeveledManifest(cfs, maxSSTableSize, options);

        // ensure all SSTables are in the manifest
        for (SSTableReader ssTableReader : sstables)
        {
            manifest.add(ssTableReader);
        }
        for (int i = 1; i < manifest.getAllLevelSize().length; i++)
        {
            manifest.repairOverlappingSSTables(i);
        }
        return manifest;
    }

    public synchronized void add(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();

        assert level < generations.length : "Invalid level " + level + " out of " + (generations.length - 1);
        logDistribution();
        if (canAddSSTable(reader))
        {
            // adding the sstable does not cause overlap in the level
            logger.info("Adding {} to L{}", reader, level);
            generations[level].add(reader);
        }
        else
        {
            // this can happen if:
            // * a compaction has promoted an overlapping sstable to the given level, or
            //   was also supposed to add an sstable at the given level.
            // * we are moving sstables from unrepaired to repaired and the sstable
            //   would cause overlap
            //
            // The add(..):ed sstable will be sent to level 0
            try
            {
                reader.descriptor.getMetadataSerializer().mutateLevel(reader.descriptor, 0);
                reader.reloadSSTableMetadata();
            }
            catch (IOException e)
            {
                logger.error("Could not change sstable level - adding it at level 0 anyway, we will find it at restart.", e);
            }

            logger.info("Can't add {} to L{}, adding to L0 instead", reader, level);
            generations[0].add(reader);
        }
    }

    public synchronized void replace(Collection<SSTableReader> removed, Collection<SSTableReader> added)
    {
        assert !removed.isEmpty(); // use add() instead of promote when adding new sstables
        logDistribution();
        if (logger.isDebugEnabled())
            logger.debug("Replacing [{}]", toString(removed));

        // the level for the added sstables is the max of the removed ones,
        // plus one if the removed were all on the same level
        int minLevel = Integer.MAX_VALUE;

        for (SSTableReader sstable : removed)
        {
            int thisLevel = remove(sstable);
            minLevel = Math.min(minLevel, thisLevel);
        }

        // it's valid to do a remove w/o an add (e.g. on truncate)
        if (added.isEmpty())
            return;

        if (logger.isDebugEnabled())
            logger.debug("Adding [{}]", toString(added));

        for (SSTableReader ssTableReader : added)
            add(ssTableReader);
        lastCompactedKeys[minLevel] = SSTableReader.sstableOrdering.max(added).last;
    }

    public synchronized void repairOverlappingSSTables(int level)
    {
        SSTableReader previous = null;
        Collections.sort(generations[level], SSTableReader.sstableComparator);
        List<SSTableReader> outOfOrderSSTables = new ArrayList<>();
        for (SSTableReader current : generations[level])
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
            {
                logger.warn(String.format("At level %d, %s [%s, %s] overlaps %s [%s, %s].  This could be caused by a bug in Cassandra 1.1.0 .. 1.1.3 or due to the fact that you have dropped sstables from another node into the data directory. " +
                                          "Sending back to L0.  If you didn't drop in sstables, and have not yet run scrub, you should do so since you may also have rows out-of-order within an sstable",
                                          level, previous, previous.first, previous.last, current, current.first, current.last));
                outOfOrderSSTables.add(current);
            }
            else
            {
                previous = current;
            }
        }

        if (!outOfOrderSSTables.isEmpty())
        {
            for (SSTableReader sstable : outOfOrderSSTables)
                sendBackToL0(sstable);
        }
    }

    /**
     * Checks if adding the sstable creates an overlap in the level
     * @param sstable the sstable to add
     * @return true if it is safe to add the sstable in the level.
     */
    private boolean canAddSSTable(SSTableReader sstable)
    {
        int level = sstable.getSSTableLevel();
        if (level == 0)
            return true;

        List<SSTableReader> copyLevel = new ArrayList<>(generations[level]);
        copyLevel.add(sstable);
        Collections.sort(copyLevel, SSTableReader.sstableComparator);

        SSTableReader previous = null;
        for (SSTableReader current : copyLevel)
        {
            if (previous != null && current.first.compareTo(previous.last) <= 0)
                return false;
            previous = current;
        }
        return true;
    }

    private synchronized void sendBackToL0(SSTableReader sstable)
    {
        remove(sstable);
        try
        {
            sstable.descriptor.getMetadataSerializer().mutateLevel(sstable.descriptor, 0);
            sstable.reloadSSTableMetadata();
            add(sstable);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Could not reload sstable meta data", e);
        }
    }

    private String toString(Collection<SSTableReader> sstables)
    {
        StringBuilder builder = new StringBuilder();
        for (SSTableReader sstable : sstables)
        {
            builder.append(sstable.descriptor.cfname)
                   .append('-')
                   .append(sstable.descriptor.generation)
                   .append("(L")
                   .append(sstable.getSSTableLevel())
                   .append("), ");
        }
        return builder.toString();
    }

    @VisibleForTesting
    long maxBytesForLevel(int level)
    {
        if (level == 0)
            return 4L * maxSSTableSizeInBytes;
        double bytes = Math.pow(10, level) * maxSSTableSizeInBytes;
        if (bytes > Long.MAX_VALUE)
            throw new RuntimeException("At most " + Long.MAX_VALUE + " bytes may be in a compaction level; your maxSSTableSize must be absurdly high to compute " + bytes);
        return (long) bytes;
    }

    /**
     * @return highest-priority sstables to compact, and level to compact them to
     * If no compactions are necessary, will return null
     */
    public synchronized CompactionCandidate getCompactionCandidates(final int gcBefore, double tombstoneThreshold)
    {
        // LevelDB gives each level a score of how much data it contains vs its ideal amount, and
        // compacts the level with the highest score. But this falls apart spectacularly once you
        // get behind.  Consider this set of levels:
        // L0: 988 [ideal: 4]
        // L1: 117 [ideal: 10]
        // L2: 12  [ideal: 100]
        //
        // The problem is that L0 has a much higher score (almost 250) than L1 (11), so what we'll
        // do is compact a batch of MAX_COMPACTING_L0 sstables with all 117 L1 sstables, and put the
        // result (say, 120 sstables) in L1. Then we'll compact the next batch of MAX_COMPACTING_L0,
        // and so forth.  So we spend most of our i/o rewriting the L1 data with each batch.
        //
        // If we could just do *all* L0 a single time with L1, that would be ideal.  But we can't
        // -- see the javadoc for MAX_COMPACTING_L0.
        //
        // LevelDB's way around this is to simply block writes if L0 compaction falls behind.
        // We don't have that luxury.
        //
        // So instead, we
        // 1) force compacting higher levels first, which minimizes the i/o needed to compact
        //    optimially which gives us a long term win, and
        // 2) if L0 falls behind, we will size-tiered compact it to reduce read overhead until
        //    we can catch up on the higher levels.
        //
        // This isn't a magic wand -- if you are consistently writing too fast for LCS to keep
        // up, you're still screwed.  But if instead you have intermittent bursts of activity,
        // it can help a lot.
        for (int i = generations.length - 1; i > 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            if (sstables.isEmpty())
                continue; // mostly this just avoids polluting the debug log with zero scores
            // we want to calculate score excluding compacting ones
            Set<SSTableReader> sstablesInLevel = Sets.newHashSet(sstables);
            Set<SSTableReader> remaining = Sets.difference(sstablesInLevel, cfs.getDataTracker().unsafeGetCompacting());
            double score = (double) SSTableReader.getTotalBytes(remaining) / (double)maxBytesForLevel(i);
            logger.debug("Compaction score for {}/{} L{} is {}", cfs.keyspace.getName(), cfs.name, i, score);

            if (score > 1.001)
            {
                // before proceeding with a higher level, let's see if L0 is far enough behind to warrant STCS
                CompactionCandidate l0Compaction = getSTCSInL0CompactionCandidate();
                if (l0Compaction != null)
                    return l0Compaction;

                // L0 is fine, proceed with this level
                Collection<SSTableReader> candidates = getSizeBasedCandidatesFor(i);
                if (!candidates.isEmpty())
                {
                    int nextLevel = getNextLevel(candidates);
                    candidates = getOverlappingStarvedSSTables(nextLevel, candidates);
                    logger.info("Compaction candidates for L{} are {}", i, toString(candidates));
                    return new CompactionCandidate(candidates, nextLevel, cfs.getCompactionStrategy().getMaxSSTableBytes());
                }
                else
                {
                    logger.debug("No compaction candidates for L{}", i);
                }
            }
        }

        // If levels are sized properly, verify if there are sstables with high tombstone ratio. SSTables with high
        // tombstone ratio are pushed up to higher levels as an attempt to compact its cells with the cells they remove,
        // therefore effectively deleting them. Tombstone push-up is an attempt to drop tombstones faster than normal C*
        // leveled compaction would drop. This strategy has a possible downside of delaying compactions at lower levels
        // if higher levels have sstables with high tombstone ratio due to accumulated tombstones prior to this patch
        // being deployed, or during sudden bursts of deletes. In those scenarios, C* will spend most its compaction
        // time performing level compactions and tombstone push-ups until the sstables are property sized and have
        // acceptable tombstone ratios.
        if (Boolean.getBoolean("fauna.tombstone-pushup")) {
            for (int i = getLevelCount() - 1; i > 0; i--)
            {
                Collection<SSTableReader> candidates = getTombstoneRatioBasedCandidatesFor(i, gcBefore, tombstoneThreshold);
                if (candidates != null)
                {
                    // before tombstone push up, check if L0 needs STCS compaction
                    CompactionCandidate l0Compaction = getSTCSInL0CompactionCandidate();
                    if (l0Compaction != null)
                        return l0Compaction;

                    int targetLevel = getNextLevel(candidates);
                    // if possible, pick a starved sstable from higher levels to re-balance the tree
                    candidates = getOverlappingStarvedSSTables(targetLevel, candidates);

                    logger.info("Compaction candidates for tombstone push-up at L{} are {}", i, toString(candidates));

                    return new CompactionCandidate(
                                                   candidates, targetLevel, cfs.getCompactionStrategy().getMaxSSTableBytes());
                }
                logger.debug("No candidates for tombstone push-up at L{}", i);
            }
        }

        // Higher levels are happy, time for a standard, non-STCS L0 compaction
        if (getLevel(0).isEmpty())
            return null;
        Collection<SSTableReader> candidates = getSizeBasedCandidatesFor(0);
        if (candidates.isEmpty())
        {
            // Since we don't have any other compactions to do, see if there is a STCS compaction to perform in L0; if
            // there is a long running compaction, we want to make sure that we continue to keep the number of SSTables
            // small in L0.
            return getSTCSInL0CompactionCandidate();
        }
        return new CompactionCandidate(candidates, getNextLevel(candidates), cfs.getCompactionStrategy().getMaxSSTableBytes());
    }

    private CompactionCandidate getSTCSInL0CompactionCandidate()
    {
        if (!DatabaseDescriptor.getDisableSTCSInL0() && getLevel(0).size() > MAX_COMPACTING_L0)
        {
            List<SSTableReader> mostInteresting = getSSTablesForSTCS(getLevel(0));
            if (!mostInteresting.isEmpty())
            {
                logger.info("L0 is too far behind, performing size-tiering there first");
                return new CompactionCandidate(mostInteresting, 0, Long.MAX_VALUE);
            }
        }

        return null;
    }

    private List<SSTableReader> getSSTablesForSTCS(Collection<SSTableReader> sstables)
    {
        Iterable<SSTableReader> candidates = cfs.getDataTracker().unsafeGetUncompactingSSTables(sstables);
        List<Pair<SSTableReader,Long>> pairs = SizeTieredCompactionStrategy.createSSTableAndLengthPairs(AbstractCompactionStrategy.filterSuspectSSTables(candidates));
        List<List<SSTableReader>> buckets = SizeTieredCompactionStrategy.getBuckets(pairs,
                                                                                    options.bucketHigh,
                                                                                    options.bucketLow,
                                                                                    options.minSSTableSize);
        return SizeTieredCompactionStrategy.mostInterestingBucket(buckets, 4, 32);
    }

    /**
     * If we do something that makes many levels contain too little data (cleanup, change sstable size) we will "never"
     * compact the high levels.
     *
     * This method finds if we have gone many compaction rounds without doing any high-level compaction, if so
     * we start bringing in one sstable from the highest level until that level is either empty or is doing compaction.
     *
     * @param targetLevel the level the candidates will be compacted into
     * @param candidates the original sstables to compact
     * @return
     */
    private Collection<SSTableReader> getOverlappingStarvedSSTables(int targetLevel, Collection<SSTableReader> candidates)
    {
        Set<SSTableReader> withStarvedCandidate = new HashSet<>(candidates);

        for (int i = generations.length - 1; i > 0; i--)
            compactionCounter[i]++;
        compactionCounter[targetLevel] = 0;
        if (logger.isDebugEnabled())
        {
            for (int j = 0; j < compactionCounter.length; j++)
                logger.debug("CompactionCounter: {}: {}", j, compactionCounter[j]);
        }

        for (int i = generations.length - 1; i > 0; i--)
        {
            if (getLevelSize(i) > 0)
            {
                if (compactionCounter[i] > NO_COMPACTION_LIMIT)
                {
                    // we try to find an sstable that is fully contained within  the boundaries we are compacting;
                    // say we are compacting 3 sstables: 0->30 in L1 and 0->12, 12->33 in L2
                    // this means that we will not create overlap in L2 if we add an sstable
                    // contained within 0 -> 33 to the compaction
                    RowPosition max = null;
                    RowPosition min = null;
                    for (SSTableReader candidate : candidates)
                    {
                        if (min == null || candidate.first.compareTo(min) < 0)
                            min = candidate.first;
                        if (max == null || candidate.last.compareTo(max) > 0)
                            max = candidate.last;
                    }
                    if (min == null || max == null || min.equals(max)) // single partition sstables - we cannot include a high level sstable.
                        return candidates;
                    Set<SSTableReader> compacting = cfs.getDataTracker().unsafeGetCompacting();
                    Range<RowPosition> boundaries = new Range<>(min, max);
                    for (SSTableReader sstable : getLevel(i))
                    {
                        Range<RowPosition> r = new Range<RowPosition>(sstable.first, sstable.last);
                        if (boundaries.contains(r) && !compacting.contains(sstable))
                        {
                            logger.info("Adding high-level (L{}) {} to candidates", sstable.getSSTableLevel(), sstable);
                            withStarvedCandidate.add(sstable);
                            return withStarvedCandidate;
                        }
                    }
                }
                return candidates;
            }
        }

        return candidates;
    }

    public synchronized int getLevelSize(int i)
    {
        if (i >= generations.length)
            throw new ArrayIndexOutOfBoundsException("Maximum valid generation is " + (generations.length - 1));
        return getLevel(i).size();
    }

    public synchronized int[] getAllLevelSize()
    {
        int[] counts = new int[generations.length];
        for (int i = 0; i < counts.length; i++)
            counts[i] = getLevel(i).size();
        return counts;
    }

    private void logDistribution()
    {
        if (logger.isDebugEnabled())
        {
            for (int i = 0; i < generations.length; i++)
            {
                if (!getLevel(i).isEmpty())
                {
                    logger.debug("L{} contains {} SSTables ({} bytes) in {}",
                                 i, getLevel(i).size(), SSTableReader.getTotalBytes(getLevel(i)), this);
                }
            }
        }
    }

    @VisibleForTesting
    public synchronized int remove(SSTableReader reader)
    {
        int level = reader.getSSTableLevel();
        assert level >= 0 : reader + " not present in manifest: "+level;
        generations[level].remove(reader);
        return level;
    }

    private static Set<SSTableReader> overlapping(Collection<SSTableReader> candidates, Iterable<SSTableReader> others)
    {
        assert !candidates.isEmpty();
        /*
         * Picking each sstable from others that overlap one of the sstable of candidates is not enough
         * because you could have the following situation:
         *   candidates = [ s1(a, c), s2(m, z) ]
         *   others = [ s3(e, g) ]
         * In that case, s2 overlaps none of s1 or s2, but if we compact s1 with s2, the resulting sstable will
         * overlap s3, so we must return s3.
         *
         * Thus, the correct approach is to pick sstables overlapping anything between the first key in all
         * the candidate sstables, and the last.
         */
        Iterator<SSTableReader> iter = candidates.iterator();
        SSTableReader sstable = iter.next();
        Token first = sstable.first.getToken();
        Token last = sstable.last.getToken();
        while (iter.hasNext())
        {
            sstable = iter.next();
            first = first.compareTo(sstable.first.getToken()) <= 0 ? first : sstable.first.getToken();
            last = last.compareTo(sstable.last.getToken()) >= 0 ? last : sstable.last.getToken();
        }
        return overlapping(first, last, others);
    }

    @VisibleForTesting
    static Set<SSTableReader> overlapping(SSTableReader sstable, Iterable<SSTableReader> others)
    {
        return overlapping(sstable.first.getToken(), sstable.last.getToken(), others);
    }

    /**
     * @return sstables from @param sstables that contain keys between @param start and @param end, inclusive.
     */
    private static Set<SSTableReader> overlapping(Token start, Token end, Iterable<SSTableReader> sstables)
    {
        assert start.compareTo(end) <= 0;
        Set<SSTableReader> overlapped = new HashSet<SSTableReader>();
        Bounds<Token> promotedBounds = new Bounds<Token>(start, end);
        for (SSTableReader candidate : sstables)
        {
            Bounds<Token> candidateBounds = new Bounds<Token>(candidate.first.getToken(), candidate.last.getToken());
            if (candidateBounds.intersects(promotedBounds))
                overlapped.add(candidate);
        }
        return overlapped;
    }

    private static final Predicate<SSTableReader> suspectP = new Predicate<SSTableReader>()
    {
        public boolean apply(SSTableReader candidate)
        {
            return candidate.isMarkedSuspect();
        }
    };

    /**
     * @return highest-priority sstables to compact for the given level.
     * If no compactions are possible (because of concurrent compactions or because some sstables are blacklisted
     * for prior failure), will return an empty list.  Never returns null.
     */
    private Collection<SSTableReader> getSizeBasedCandidatesFor(int level)
    {
        assert !getLevel(level).isEmpty();
        logger.debug("Choosing candidates for L{}", level);

        final Set<SSTableReader> compacting = cfs.getDataTracker().unsafeGetCompacting();

        if (level == 0)
        {
            Set<SSTableReader> compactingL0 = getCompacting(0);

            RowPosition lastCompactingKey = null;
            RowPosition firstCompactingKey = null;
            for (SSTableReader candidate : compactingL0)
            {
                if (firstCompactingKey == null || candidate.first.compareTo(firstCompactingKey) < 0)
                    firstCompactingKey = candidate.first;
                if (lastCompactingKey == null || candidate.last.compareTo(lastCompactingKey) > 0)
                    lastCompactingKey = candidate.last;
            }

            // L0 is the dumping ground for new sstables which thus may overlap each other.
            //
            // We treat L0 compactions specially:
            // 1a. add sstables to the candidate set until we have at least maxSSTableSizeInMB
            // 1b. prefer choosing older sstables as candidates, to newer ones
            // 1c. any L0 sstables that overlap a candidate, will also become candidates
            // 2. At most MAX_COMPACTING_L0 sstables from L0 will be compacted at once
            // 3. If total candidate size is less than maxSSTableSizeInMB, we won't bother compacting with L1,
            //    and the result of the compaction will stay in L0 instead of being promoted (see promote())
            //
            // Note that we ignore suspect-ness of L1 sstables here, since if an L1 sstable is suspect we're
            // basically screwed, since we expect all or most L0 sstables to overlap with each L1 sstable.
            // So if an L1 sstable is suspect we can't do much besides try anyway and hope for the best.
            Set<SSTableReader> candidates = new HashSet<SSTableReader>();
            Set<SSTableReader> remaining = new HashSet<SSTableReader>();
            Iterables.addAll(remaining, Iterables.filter(getLevel(0), Predicates.not(suspectP)));
            for (SSTableReader sstable : ageSortedSSTables(remaining))
            {
                if (candidates.contains(sstable))
                    continue;

                Sets.SetView<SSTableReader> overlappedL0 = Sets.union(Collections.singleton(sstable), overlapping(sstable, remaining));
                if (!Sets.intersection(overlappedL0, compactingL0).isEmpty())
                    continue;

                for (SSTableReader newCandidate : overlappedL0)
                {
                    if (firstCompactingKey == null || lastCompactingKey == null || overlapping(firstCompactingKey.getToken(), lastCompactingKey.getToken(), Arrays.asList(newCandidate)).size() == 0)
                        candidates.add(newCandidate);
                    remaining.remove(newCandidate);
                }

                if (candidates.size() > MAX_COMPACTING_L0)
                {
                    // limit to only the MAX_COMPACTING_L0 oldest candidates
                    candidates = new HashSet<>(ageSortedSSTables(candidates).subList(0, MAX_COMPACTING_L0));
                    break;
                }
            }

            // leave everything in L0 if we didn't end up with a full sstable's worth of data
            if (SSTableReader.getTotalBytes(candidates) > maxSSTableSizeInBytes)
            {
                // add sstables from L1 that overlap candidates
                // if the overlapping ones are already busy in a compaction, leave it out.
                // TODO try to find a set of L0 sstables that only overlaps with non-busy L1 sstables
                Set<SSTableReader> l1overlapping = overlapping(candidates, getLevel(1));
                if (Sets.intersection(l1overlapping, compacting).size() > 0)
                    return Collections.emptyList();
                if (!overlapping(candidates, compactingL0).isEmpty())
                    return Collections.emptyList();
                candidates = Sets.union(candidates, l1overlapping);
            }


            if (candidates.size() < minL0Compacting)
                return Collections.emptyList();
            else
                return candidates;
        }

        // for non-L0 compactions, pick up where we left off last time
        Collections.sort(getLevel(level), SSTableReader.sstableComparator);
        int start = 0; // handles case where the prior compaction touched the very last range
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get(i);
            if (sstable.first.compareTo(lastCompactedKeys[level]) > 0)
            {
                start = i;
                break;
            }
        }

        double nextSize = (double)SSTableReader.getTotalBytes(getLevel(level + 1));

        // look for a non-suspect keyspace to compact with, starting with where we left off last time,
        // and wrapping back to the beginning of the generation if necessary
        for (int i = 0; i < getLevel(level).size(); i++)
        {
            SSTableReader sstable = getLevel(level).get((start + i) % getLevel(level).size());
            Set<SSTableReader> candidates = Sets.union(Collections.singleton(sstable), overlapping(sstable, getLevel(level + 1)));
            if (Iterables.any(candidates, suspectP))
                continue;

            // Allow this candidate if it won't compact too many files.
            if (candidates.size() <= maxSplashFiles || Boolean.getBoolean(ALLOW_SPLASH_OPTION)) {
                logger.info("Splash allowing this compaction for {} files for max of {}. ",
                        candidates.size(), maxSplashFiles, toString(candidates));
            } else {
                logger.info("Splash would compact too many files {} for max of {}. ",
                        candidates.size(), maxSplashFiles, toString(candidates));
                continue;
            }

            if (Sets.intersection(candidates, compacting).isEmpty())
                return candidates;
        }

        // all the sstables were suspect or overlapped with something suspect
        return Collections.emptyList();
    }

    private Collection<SSTableReader> getTombstoneRatioBasedCandidatesFor(int level, final int gcBefore, double tombstoneThreshold)
    {
        // [1, N), where N has sstables. We can't push tombstones any higher.
        assert level > 0 && level < getLevelCount() : "Invalid tombstone push-up level";

        Set<SSTableReader> compacting = cfs.getDataTracker().unsafeGetCompacting();
        SortedSet<SSTableReader> sstables = getLevelSorted(level, new Comparator<SSTableReader>()
        {
            public int compare(SSTableReader a, SSTableReader b)
            {
                return -1 * Double.compare(
                    a.getEstimatedDroppableTombstoneRatio(gcBefore),
                    b.getEstimatedDroppableTombstoneRatio(gcBefore));
            }
        });

        for (SSTableReader sstable : sstables)
        {
            if (sstable.getEstimatedDroppableTombstoneRatio(gcBefore) <= tombstoneThreshold)
                break; // there can be no more sstables with high thombstone ratio after this entry

            if (compacting.contains(sstable))
                continue; // this sstable is already compacting

            Set<SSTableReader> candidates = Sets.union(
                Collections.singleton(sstable),
                overlapping(sstable, getLevel(level + 1))
            );

            // skip single sstable compaction, already compacting sstables, or suspected sstables
            if (candidates.size() < 2
                || !Sets.intersection(candidates, compacting).isEmpty()
                || Iterables.any(candidates, suspectP))
                continue;

            return candidates;
        }

        return null;
    }

    private Set<SSTableReader> getCompacting(int level)
    {
        Set<SSTableReader> sstables = new HashSet<>();
        Set<SSTableReader> levelSSTables = new HashSet<>(getLevel(level));
        for (SSTableReader sstable : cfs.getDataTracker().unsafeGetCompacting())
        {
            if (levelSSTables.contains(sstable))
                sstables.add(sstable);
        }
        return sstables;
    }

    private List<SSTableReader> ageSortedSSTables(Collection<SSTableReader> candidates)
    {
        List<SSTableReader> ageSortedCandidates = new ArrayList<SSTableReader>(candidates);
        Collections.sort(ageSortedCandidates, SSTableReader.maxTimestampComparator);
        return ageSortedCandidates;
    }

    public synchronized Set<SSTableReader>[] getSStablesPerLevelSnapshot()
    {
        Set<SSTableReader>[] sstablesPerLevel = new Set[generations.length];
        for (int i = 0; i < generations.length; i++)
        {
            sstablesPerLevel[i] = new HashSet<>(generations[i]);
        }
        return sstablesPerLevel;
    }

    @Override
    public String toString()
    {
        return "Manifest@" + hashCode();
    }

    public int getLevelCount()
    {
        for (int i = generations.length - 1; i >= 0; i--)
        {
            if (getLevel(i).size() > 0)
                return i;
        }
        return 0;
    }

    public synchronized SortedSet<SSTableReader> getLevelSorted(int level, Comparator<SSTableReader> comparator)
    {
        return ImmutableSortedSet.copyOf(comparator, getLevel(level));
    }

    public List<SSTableReader> getLevel(int i)
    {
        return generations[i];
    }

    public synchronized int getEstimatedTasks()
    {
        long tasks = 0;
        long[] estimated = new long[generations.length];

        for (int i = generations.length - 1; i >= 0; i--)
        {
            List<SSTableReader> sstables = getLevel(i);
            // If there is 1 byte over TBL - (MBL * 1.001), there is still a task left, so we need to round up.
            estimated[i] = (long)Math.ceil((double)Math.max(0L, SSTableReader.getTotalBytes(sstables) - (long)(maxBytesForLevel(i) * 1.001)) / (double)maxSSTableSizeInBytes);
            tasks += estimated[i];
        }

        logger.debug("Estimating {} compactions to do for {}.{}",
                     Arrays.toString(estimated), cfs.keyspace.getName(), cfs.name);
        return Ints.checkedCast(tasks);
    }

    public int getNextLevel(Collection<SSTableReader> sstables)
    {
        int maximumLevel = Integer.MIN_VALUE;
        int minimumLevel = Integer.MAX_VALUE;
        for (SSTableReader sstable : sstables)
        {
            maximumLevel = Math.max(sstable.getSSTableLevel(), maximumLevel);
            minimumLevel = Math.min(sstable.getSSTableLevel(), minimumLevel);
        }

        int newLevel;
        if (minimumLevel == 0 && minimumLevel == maximumLevel && SSTableReader.getTotalBytes(sstables) < maxSSTableSizeInBytes)
        {
            newLevel = 0;
        }
        else
        {
            newLevel = minimumLevel == maximumLevel ? maximumLevel + 1 : maximumLevel;
            assert newLevel > 0;
        }
        return newLevel;

    }

    public static class CompactionCandidate
    {
        public final Collection<SSTableReader> sstables;
        public final int level;
        public final long maxSSTableBytes;

        public CompactionCandidate(Collection<SSTableReader> sstables, int level, long maxSSTableBytes)
        {
            this.sstables = sstables;
            this.level = level;
            this.maxSSTableBytes = maxSSTableBytes;
        }
    }
}
