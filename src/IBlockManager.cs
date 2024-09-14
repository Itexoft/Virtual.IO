//-----------------------------------------------------------------------
// <copyright company="Itexoft">
//   Copyright (c) Itexoft. All Rights Reserved.
//   Author: Denis Kudelin
//-----------------------------------------------------------------------

namespace Itexoft.Virtual.IO
{
    /// <summary>
    /// Defines methods for allocating, freeing, reading, and writing blocks of data.
    /// </summary>
    internal interface IBlockManager
    {
        /// <summary>
        /// Gets block size.
        /// </summary>
        int BlockSize { get; }

        /// <summary>
        /// Allocates a new block and returns its index.
        /// </summary>
        /// <returns>The index of the allocated block.</returns>
        long AllocateBlock();

        /// <summary>
        /// Frees the specified block.
        /// </summary>
        /// <param name="blockIndex">The index of the block to free.</param>
        void FreeBlock(long blockIndex);

        /// <summary>
        /// Writes data to the specified block.
        /// </summary>
        /// <param name="blockIndex">The index of the block to write to.</param>
        /// <param name="positionOffset">The byte offset in the block at which to begin writing data.</param>
        /// <param name="data">The data to write.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="data"/> at which to begin copying bytes to the block.</param>
        /// <param name="count">The number of bytes to write.</param>
        void WriteBlock(long blockIndex, int positionOffset, byte[] data, int offset, int count);

        /// <summary>
        /// Reads data from the specified block.
        /// </summary>
        /// <param name="blockIndex">The index of the block to read from.</param>
        /// <param name="positionOffset">The byte offset in the block at which to begin reading data.</param>
        /// <param name="buffer">The buffer to store the read data.</param>
        /// <param name="offset">The zero-based byte offset in <paramref name="buffer"/> at which to begin storing the data read from the block.</param>
        /// <param name="count">The number of bytes to read.</param>
        void ReadBlock(long blockIndex, int positionOffset, byte[] buffer, int offset, int count);

        /// <summary>
        /// Executes batch read/write operations on the specified block.
        /// </summary>
        /// <param name="action">The action containing batch read and write operations.</param>
        /// <remarks>
        /// The <paramref name="action"/> accepts two delegates:
        /// - A read delegate that allows reading data from the block.
        /// - A write delegate that allows writing data to the block.
        /// Both delegates have the same signature:
        /// <para>
        /// <c>int BatchDelegate(int positionOffset, byte[] buffer, int offset, int count)</c>
        /// </para>
        /// </remarks>
        void Batch(Action<IBlockManager> action);
    }
}
