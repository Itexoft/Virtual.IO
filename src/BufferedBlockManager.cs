//-----------------------------------------------------------------------
// <copyright company="Itexoft">
//   Copyright (c) Itexoft. All Rights Reserved.
//   Author: Denis Kudelin
//-----------------------------------------------------------------------

namespace Itexoft.Virtual.IO
{
    /// <summary>
    /// BufferedBlockManager adds a caching layer to improve read performance by caching the last three read blocks.
    /// </summary>
    internal sealed class BufferedBlockManager : IBlockManager
    {
        private readonly IBlockManager blockManager;

        private long cachedBlockIndex1 = -1;
        private readonly byte[] cachedBlockData1;
        private long cacheAccess1 = 0;

        private long cachedBlockIndex2 = -1;
        private readonly byte[] cachedBlockData2;
        private long cacheAccess2 = 0;

        private long cachedBlockIndex3 = -1;
        private readonly byte[] cachedBlockData3;
        private long cacheAccess3 = 0;

        private long accessCounter = 0;
        private BatchBlockManager batchBlockManager;

        internal BufferedBlockManager(IBlockManager blockManager)
        {
            this.blockManager = blockManager ?? throw new ArgumentNullException(nameof(blockManager));

            var blockSize = this.blockManager.BlockSize;
            this.cachedBlockData1 = new byte[blockSize];
            this.cachedBlockData2 = new byte[blockSize];
            this.cachedBlockData3 = new byte[blockSize];
        }

        /// <inheritdoc/>
        public int BlockSize => this.blockManager.BlockSize;

        /// <inheritdoc/>
        public long AllocateBlock() => this.blockManager.AllocateBlock();

        /// <inheritdoc/>
        public void FreeBlock(long blockIndex)
        {
            this.FreeBlockInternal(this.blockManager, blockIndex);
        }

        /// <inheritdoc/>
        public void Batch(Action<IBlockManager> action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));

            this.blockManager.Batch((blockManager) =>
            {
                this.batchBlockManager ??= new BatchBlockManager(
                        () => blockManager.BlockSize,
                        blockManager.AllocateBlock,
                        bi => this.FreeBlockInternal(blockManager, bi),
                        (bi, p, b, o, c) => this.ReadBlockInternal(blockManager, bi, p, b, o, c),
                        (bi, p, b, o, c) => this.WriteBlockInternal(blockManager, bi, p, b, o, c));

                action(this.batchBlockManager);
            });
        }

        /// <inheritdoc/>
        public void ReadBlock(long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            this.ReadBlockInternal(this.blockManager, blockIndex, positionOffset, buffer, offset, count);
        }

        /// <inheritdoc/>
        public void WriteBlock(long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            this.WriteBlockInternal(this.blockManager, blockIndex, positionOffset, buffer, offset, count);
        }

        private void ReadBlockInternal(IBlockManager blockManager, long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            if (this.TryReadFromCache(blockIndex, positionOffset, buffer, offset, count))
            {
                return;
            }

            var cacheData = this.GetLeastRecentlyUsedCacheData(out var lruSlot);

            blockManager.ReadBlock(blockIndex, 0, cacheData, 0, this.BlockSize);

            this.SetCacheSlot(lruSlot, blockIndex);

            Array.Copy(cacheData, positionOffset, buffer, offset, count);
        }

        public void FreeBlockInternal(IBlockManager blockManager, long blockIndex)
        {
            blockManager.FreeBlock(blockIndex);
            this.InvalidateCache(blockIndex);
        }

        private void WriteBlockInternal(IBlockManager blockManager, long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            blockManager.WriteBlock(blockIndex, positionOffset, buffer, offset, count);
            this.UpdateCacheAfterWrite(blockIndex, positionOffset, buffer, offset, count);
        }

        private bool TryReadFromCache(long blockIndex, int positionOffset, byte[] buffer, int bufferOffset, int count)
        {
            if (blockIndex == this.cachedBlockIndex1)
            {
                this.UpdateAccessCounter(ref this.cacheAccess1);
                Array.Copy(this.cachedBlockData1, positionOffset, buffer, bufferOffset, count);
                return true;
            }

            if (blockIndex == this.cachedBlockIndex2)
            {
                this.UpdateAccessCounter(ref this.cacheAccess2);
                Array.Copy(this.cachedBlockData2, positionOffset, buffer, bufferOffset, count);
                return true;
            }

            if (blockIndex == this.cachedBlockIndex3)
            {
                this.UpdateAccessCounter(ref this.cacheAccess3);
                Array.Copy(this.cachedBlockData3, positionOffset, buffer, bufferOffset, count);
                return true;
            }

            return false;
        }

        private void UpdateCacheAfterWrite(long blockIndex, int positionOffset, byte[] data, int offset, int count)
        {
            byte[] cacheData = null;

            if (blockIndex == this.cachedBlockIndex1)
            {
                cacheData = this.cachedBlockData1;
                this.UpdateAccessCounter(ref this.cacheAccess1);
            }
            else if (blockIndex == this.cachedBlockIndex2)
            {
                cacheData = this.cachedBlockData2;
                this.UpdateAccessCounter(ref this.cacheAccess2);
            }
            else if (blockIndex == this.cachedBlockIndex3)
            {
                cacheData = this.cachedBlockData3;
                this.UpdateAccessCounter(ref this.cacheAccess3);
            }

            if (cacheData != null)
            {
                Array.Copy(data, offset, cacheData, positionOffset, count);
            }
        }

        private void InvalidateCache(long blockIndex)
        {
            if (blockIndex == this.cachedBlockIndex1)
            {
                this.cachedBlockIndex1 = -1;
            }
            else if (blockIndex == this.cachedBlockIndex2)
            {
                this.cachedBlockIndex2 = -1;
            }
            else if (blockIndex == this.cachedBlockIndex3)
            {
                this.cachedBlockIndex3 = -1;
            }
        }

        private byte[] GetLeastRecentlyUsedCacheData(out int lruSlot)
        {
            lruSlot = this.GetLeastRecentlyUsedSlot();
            return this.GetCacheData(lruSlot);
        }

        private int GetLeastRecentlyUsedSlot()
        {
            if (this.cacheAccess1 <= this.cacheAccess2 && this.cacheAccess1 <= this.cacheAccess3)
            {
                return 1;
            }
            else if (this.cacheAccess2 <= this.cacheAccess1 && this.cacheAccess2 <= this.cacheAccess3)
            {
                return 2;
            }
            else
            {
                return 3;
            }
        }

        private byte[] GetCacheData(int slot)
        {
            return slot switch
            {
                1 => this.cachedBlockData1,
                2 => this.cachedBlockData2,
                3 => this.cachedBlockData3,
                _ => throw new InvalidOperationException("Invalid cache slot."),
            };
        }

        private void SetCacheSlot(int slot, long blockIndex)
        {
            switch (slot)
            {
                case 1:
                    this.cachedBlockIndex1 = blockIndex;
                    this.UpdateAccessCounter(ref this.cacheAccess1);
                    break;
                case 2:
                    this.cachedBlockIndex2 = blockIndex;
                    this.UpdateAccessCounter(ref this.cacheAccess2);
                    break;
                case 3:
                    this.cachedBlockIndex3 = blockIndex;
                    this.UpdateAccessCounter(ref this.cacheAccess3);
                    break;
                default:
                    throw new InvalidOperationException("Invalid cache slot.");
            }
        }

        private void UpdateAccessCounter(ref long cacheAccess)
        {
            if (++this.accessCounter == long.MaxValue)
                this.accessCounter = 0;

            cacheAccess = this.accessCounter;
        }
    }
}