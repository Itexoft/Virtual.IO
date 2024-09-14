//-----------------------------------------------------------------------
// <copyright company="Itexoft">
//   Copyright (c) Itexoft. All Rights Reserved.
//   Author: Denis Kudelin
//-----------------------------------------------------------------------

namespace Itexoft.Virtual.IO
{
    internal sealed class BatchBlockManager : IBlockManager
    {
        // Delegates for each method and property in the IBlockManager interface
        private readonly Func<int> getBlockSize;
        private readonly Func<long> allocateBlock;
        private readonly Action<long> freeBlock;
        private readonly Action<long, int, byte[], int, int> writeBlock;
        private readonly Action<long, int, byte[], int, int> readBlock;

        // Constructor accepting delegates for each method/property
        public BatchBlockManager(
            Func<int> getBlockSize,
            Func<long> allocateBlock,
            Action<long> freeBlock,
            Action<long, int, byte[], int, int> readBlock,
            Action<long, int, byte[], int, int> writeBlock)
        {
            this.getBlockSize = getBlockSize ?? throw new ArgumentNullException(nameof(getBlockSize));
            this.allocateBlock = allocateBlock ?? throw new ArgumentNullException(nameof(allocateBlock));
            this.freeBlock = freeBlock ?? throw new ArgumentNullException(nameof(freeBlock));
            this.readBlock = readBlock ?? throw new ArgumentNullException(nameof(readBlock));
            this.writeBlock = writeBlock ?? throw new ArgumentNullException(nameof(writeBlock));
        }

        // Implementation of the IBlockManager interface using delegates
        public int BlockSize => this.getBlockSize();

        public long AllocateBlock() => this.allocateBlock();

        public void FreeBlock(long blockIndex) => this.freeBlock(blockIndex);

        public void WriteBlock(long blockIndex, int positionOffset, byte[] data, int offset, int count) => this.writeBlock(blockIndex, positionOffset, data, offset, count);

        public void ReadBlock(long blockIndex, int positionOffset, byte[] buffer, int offset, int count) => this.readBlock(blockIndex, positionOffset, buffer, offset, count);

        public void Batch(Action<IBlockManager> action) => throw new NotSupportedException();
    }
}
