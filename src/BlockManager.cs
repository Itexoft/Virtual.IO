//-----------------------------------------------------------------------
// <copyright company="Itexoft">
//   Copyright (c) Itexoft. All Rights Reserved.
//   Author: Denis Kudelin
//-----------------------------------------------------------------------

using System.Runtime.CompilerServices;

namespace Itexoft.Virtual.IO
{
    /// <summary>
    /// Manages blocks of data within a stream, allowing allocation, deallocation, reading, and writing of fixed-size blocks.
    /// </summary>
    internal sealed class BlockManager : IBlockManager
    {
        // Constants
        private const int BitsPerByte = 8;
        private const int HeaderSize = sizeof(long); // batSegmentCount

        // Fields
        private readonly int blockSize;
        private readonly Stream baseStream;
        private readonly object syncRoot = new();

        private readonly int blocksPerSegment;
        private readonly int batSegmentSizeInBytes;
        private readonly long segmentSize; // Size of each segment including BAT and data blocks
        private long batSegmentCount;

        private long getBlockDataPositionLastIndex = -1;
        private long getBlockDataPositionLastValue;
        private readonly BatchBlockManager batchBlockManager;

        /// <summary>
        /// Initializes a new instance of the <see cref="BlockManager"/> class.
        /// </summary>
        /// <param name="stream">The stream used for data storage.</param>
        /// <param name="blockSize">The size of each block in bytes.</param>
        /// <param name="blocksPerSegment">The number of blocks per BAT segment.</param>
        public BlockManager(Stream stream, int blockSize)
        {
            if (stream == null)
                throw new ArgumentNullException(nameof(stream));

            if (!stream.CanSeek)
                throw new ArgumentException("Stream must support seeking.", nameof(stream));

            if (!stream.CanRead || !stream.CanWrite)
                throw new ArgumentException("Stream must support reading and writing.", nameof(stream));

            if (blockSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(blockSize), "Block size must be positive.");

            this.baseStream = stream;
            this.blockSize = blockSize;
            this.blocksPerSegment = Math.Max(32, this.BlockSize / 32);
            this.batSegmentSizeInBytes = (this.blocksPerSegment + BitsPerByte - 1) / BitsPerByte;
            this.segmentSize = this.batSegmentSizeInBytes + ((long)this.blocksPerSegment * this.blockSize);
            this.batchBlockManager = new BatchBlockManager(() => this.blockSize, this.AllocateBlockInternal, this.FreeBlockInternal, this.ReadBlockInternal, this.WriteBlockInternal);
            this.InitializeMetadata();
        }

        /// <summary>
        /// Gets total blocks.
        /// </summary>
        internal long TotalBlocks => this.batSegmentCount * this.blocksPerSegment;

        /// <summary>
        /// Gets allocated blocks.
        /// </summary>
        internal long AllocatedBlocks
        {
            get
            {
                lock (this.syncRoot)
                {
                    var counter = 0;
                    for (long i = 0, totalBlocks = this.TotalBlocks; i < totalBlocks; i++)
                    {
                        if (this.IsBlockAllocated(i))
                            counter++;
                    }

                    return counter;
                }
            }
        }

        /// <inheritdoc/>
        public int BlockSize => this.blockSize;

        /// <inheritdoc/>
        public long AllocateBlock()
        {
            lock (this.syncRoot)
                return this.AllocateBlockInternal();
        }

        /// <inheritdoc/>
        public void FreeBlock(long blockIndex)
        {
            lock (this.syncRoot)
                this.FreeBlockInternal(blockIndex);
        }

        /// <inheritdoc/>
        public void WriteBlock(long blockIndex, int positionOffset, byte[] data, int offset, int count)
        {
            lock (this.syncRoot)
                this.WriteBlockInternal(blockIndex, positionOffset, data, offset, count);
        }

        /// <inheritdoc/>
        public void ReadBlock(long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            lock (this.syncRoot)
                this.ReadBlockInternal(blockIndex, positionOffset, buffer, offset, count);
        }

        /// <inheritdoc/>
        public void Batch(Action<IBlockManager> action)
        {
            lock (this.syncRoot)
                this.BatchInternal(action);
        }

        private long AllocateBlockInternal()
        {
            while (true)
            {
                var blockIndex = this.FindFreeBlock();
                if (blockIndex != -1)
                {
                    this.MarkBlockAsAllocated(blockIndex);
                    return blockIndex;
                }

                // Need to expand BAT
                this.ExpandBat();
            }
        }

        private void FreeBlockInternal(long blockIndex)
        {
            this.ValidateBlockIndex(blockIndex);

            if (!this.IsBlockAllocated(blockIndex))
                throw new InvalidOperationException("Block is already free.");

            this.MarkBlockAsFree(blockIndex);
        }

        public void ReadBlockInternal(long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            this.ValidateBlockParameters(blockIndex, positionOffset, buffer, offset, count);

            var position = this.GetBlockDataPosition(blockIndex);
            this.baseStream.Seek(position + positionOffset, SeekOrigin.Begin);
            var bytesRead = this.baseStream.Read(buffer, offset, count);

            if (bytesRead < count)
                throw new EndOfStreamException("Unable to read the complete block data.");
        }

        public void WriteBlockInternal(long blockIndex, int positionOffset, byte[] data, int offset, int count)
        {
            this.ValidateBlockParameters(blockIndex, positionOffset, data, offset, count);

            var position = this.GetBlockDataPosition(blockIndex);
            this.baseStream.Seek(position + positionOffset, SeekOrigin.Begin);
            this.baseStream.Write(data, offset, count);
            this.baseStream.Flush();
        }

        public void BatchInternal(Action<IBlockManager> action)
        {
            if (action == null)
                throw new ArgumentNullException(nameof(action));
            action(this.batchBlockManager);
        }

        private void ValidateBlockParameters(long blockIndex, int positionOffset, byte[] buffer, int offset, int count)
        {
            this.ValidateBlockIndex(blockIndex);

            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));

            if (offset < 0 || count < 0 || offset + count > buffer.Length)
                throw new ArgumentOutOfRangeException("Invalid offset or count.");

            if (positionOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(positionOffset));

            if (positionOffset + count > this.BlockSize)
                throw new ArgumentException("Data length exceeds block size.");
        }

        // Private Helper Methods

        private void InitializeMetadata()
        {
            if (this.baseStream.Length >= HeaderSize)
            {
                this.baseStream.Seek(0, SeekOrigin.Begin);
                var buffer = new byte[HeaderSize];
                var bytesRead = this.baseStream.Read(buffer, 0, buffer.Length);
                if (bytesRead == HeaderSize)
                {
                    this.batSegmentCount = BitConverter.ToInt64(buffer, 0);
                }
                else
                {
                    this.InitializeNewBatSegment();
                }
            }
            else
            {
                this.InitializeNewBatSegment();
            }
        }

        private void InitializeNewBatSegment()
        {
            this.batSegmentCount = 1;
            this.UpdateHeader();

            // Ensure the stream is long enough to hold the initial segment
            var initialSegmentPosition = this.GetSegmentPosition(0);
            var newLength = initialSegmentPosition + this.segmentSize;
            if (this.baseStream.Length < newLength)
            {
                this.baseStream.SetLength(newLength);
            }

            // Initialize the BAT segment to zeros
            var zeroBytes = new byte[this.batSegmentSizeInBytes];
            this.baseStream.Seek(this.GetBatSegmentPosition(0), SeekOrigin.Begin);
            this.baseStream.Write(zeroBytes, 0, zeroBytes.Length);
            this.baseStream.Flush();
        }

        private void UpdateHeader()
        {
            this.baseStream.Seek(0, SeekOrigin.Begin);
            var buffer = new byte[HeaderSize];
            BitConverter.TryWriteBytes(buffer, this.batSegmentCount);
            this.baseStream.Write(buffer, 0, buffer.Length);
            this.baseStream.Flush();
        }

        private void ValidateBlockIndex(long blockIndex)
        {
            if (blockIndex < 0 || blockIndex >= this.TotalBlocks)
                throw new ArgumentOutOfRangeException(nameof(blockIndex), "Invalid block index.");
        }

        private long FindFreeBlock()
        {
            for (long i = 0, totalBlocks = this.TotalBlocks; i < totalBlocks; i++)
            {
                if (!this.IsBlockAllocated(i))
                {
                    return i;
                }
            }

            return -1;
        }

        private bool IsBlockAllocated(long blockIndex)
        {
            var (segmentIndex, byteIndex, bitOffset) = this.GetBatPosition(blockIndex);

            var batByte = this.ReadBatByte(segmentIndex, byteIndex);
            var mask = (byte)(1 << bitOffset);

            return (batByte & mask) != 0;
        }

        private void MarkBlockAsAllocated(long blockIndex)
        {
            var (segmentIndex, byteIndex, bitOffset) = this.GetBatPosition(blockIndex);

            var batByte = this.ReadBatByte(segmentIndex, byteIndex);
            var mask = (byte)(1 << bitOffset);
            batByte |= mask;
            this.WriteBatByte(segmentIndex, byteIndex, batByte);
            this.UpdateHeader();
        }

        private void MarkBlockAsFree(long blockIndex)
        {
            var (segmentIndex, byteIndex, bitOffset) = this.GetBatPosition(blockIndex);

            var batByte = this.ReadBatByte(segmentIndex, byteIndex);
            var mask = (byte)~(1 << bitOffset);
            batByte &= mask;
            this.WriteBatByte(segmentIndex, byteIndex, batByte);
        }

        private void ExpandBat()
        {
            // Add a new BAT segment
            this.batSegmentCount++;
            this.UpdateHeader();

            // Calculate the position of the new segment
            var newSegmentPosition = this.GetSegmentPosition(this.batSegmentCount - 1);
            var newLength = newSegmentPosition + this.segmentSize;
            if (this.baseStream.Length < newLength)
            {
                this.baseStream.SetLength(newLength);
            }

            // Initialize the new BAT segment to zeros
            var zeroBytes = new byte[this.batSegmentSizeInBytes];
            this.baseStream.Seek(this.GetBatSegmentPosition(this.batSegmentCount - 1), SeekOrigin.Begin);
            this.baseStream.Write(zeroBytes, 0, zeroBytes.Length);
            this.baseStream.Flush();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private byte ReadBatByte(long segmentIndex, long byteIndex)
        {
            var position = this.GetBatSegmentPosition(segmentIndex) + byteIndex;
            this.baseStream.Seek(position, SeekOrigin.Begin);
            var value = this.baseStream.ReadByte();
            return value == -1 ? (byte)0 : (byte)value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void WriteBatByte(long segmentIndex, long byteIndex, byte value)
        {
            var position = this.GetBatSegmentPosition(segmentIndex) + byteIndex;
            this.baseStream.Seek(position, SeekOrigin.Begin);
            this.baseStream.WriteByte(value);
            this.baseStream.Flush();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetSegmentPosition(long segmentIndex)
        {
            // Segments are stored immediately after the header
            return HeaderSize + segmentIndex * this.segmentSize;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetBatSegmentPosition(long segmentIndex)
        {
            // BAT segment is at the beginning of the segment
            return this.GetSegmentPosition(segmentIndex);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long GetBlockDataPosition(long blockIndex)
        {
            if(this.getBlockDataPositionLastIndex == blockIndex)
                return this.getBlockDataPositionLastValue;

            var (segmentIndex, blockOffset) = this.GetSegmentAndBlockOffset(blockIndex);
            var segmentPosition = this.GetSegmentPosition(segmentIndex);
            // Data blocks start after the BAT segment within the segment
            var dataStartPosition = segmentPosition + this.batSegmentSizeInBytes;
            this.getBlockDataPositionLastIndex = blockIndex;
            this.getBlockDataPositionLastValue = dataStartPosition + blockOffset * this.BlockSize;
            return this.getBlockDataPositionLastValue;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (long segmentIndex, int blockOffset) GetSegmentAndBlockOffset(long blockIndex)
        {
            var segmentIndex = blockIndex / this.blocksPerSegment;
            var blockOffset = (int)(blockIndex % this.blocksPerSegment);
            return (segmentIndex, blockOffset);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private (long segmentIndex, long byteIndex, int bitOffset) GetBatPosition(long blockIndex)
        {
            var (segmentIndex, blockOffset) = this.GetSegmentAndBlockOffset(blockIndex);
            var bitIndex = blockOffset;
            var byteIndex = bitIndex / BitsPerByte;
            var bitOffset = bitIndex % BitsPerByte;
            return (segmentIndex, byteIndex, bitOffset);
        }
    }
}