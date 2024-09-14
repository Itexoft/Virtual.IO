using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;

namespace Itexoft.Virtual.IO.Tests
{
    [TestFixture]
    internal class BlockStreamTest
    {
        private Stream stream;
        private BlockManager blockManager;

        // Thread-local Random instance for thread-safe random number generation
        private static readonly ThreadLocal<Random> ThreadRandom = new(() => new Random(Guid.NewGuid().GetHashCode()));

        [SetUp]
        public void Setup()
        {
            // Initialization of the stream and blockManager will be done in each test with appropriate block size
        }

        [TearDown]
        public void Teardown()
        {
            this.stream?.Dispose();
            if (File.Exists("test.bin"))
            {
                File.Delete("test.bin");
            }
        }

        /// <summary>
        /// Tests single-threaded read and write operations to ensure data integrity.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(65536)]
        public void SingleThreaded_ReadWriteStream_ShouldMaintainDataIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 10]; // Write 10 blocks worth of data
                new Random().NextBytes(dataToWrite);

                // Act
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);
                blockStream.Seek(0, SeekOrigin.Begin);
                var buffer = new byte[dataToWrite.Length];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);

                // Assert
                Assert.That(bytesRead, Is.EqualTo(dataToWrite.Length), "Bytes read should match bytes written.");
                Assert.That(buffer, Is.EqualTo(dataToWrite), "Data read should match data written.");
            }
        }

        /// <summary>
        /// Tests multi-threaded read and write operations to ensure data integrity under concurrent access.
        /// Each thread uses its own BlockStream instance.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void MultiThreaded_MultipleStreams_ShouldMaintainDataIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var streamCount = 10;
            var blockStreams = new ConcurrentBag<BlockStream>();
            var dataDictionary = new ConcurrentDictionary<long, byte[]>();
            var exceptions = new ConcurrentBag<Exception>();

            try
            {
                // Act: Create multiple BlockStream instances and write data concurrently
                Parallel.For(0, streamCount, i =>
                {
                    try
                    {
                        var blockStream = new BlockStream(this.blockManager);
                        blockStreams.Add(blockStream);
                        var dataToWrite = new byte[blockSize * 5]; // Spans 5 blocks
                        ThreadRandom.Value.NextBytes(dataToWrite);
                        blockStream.Write(dataToWrite, 0, dataToWrite.Length);
                        dataDictionary[blockStream.StartBlockIndex] = dataToWrite;
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                });

                // Assert: No exceptions during writing
                if (!exceptions.IsEmpty)
                {
                    throw new AggregateException(exceptions);
                }

                // Act: Read back data concurrently and verify
                Parallel.ForEach(blockStreams, blockStream =>
                {
                    try
                    {
                        var startIndex = blockStream.StartBlockIndex;
                        var expectedData = dataDictionary[startIndex];
                        var buffer = new byte[expectedData.Length];
                        blockStream.Seek(0, SeekOrigin.Begin);
                        var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                        Assert.That(bytesRead, Is.EqualTo(expectedData.Length), $"Bytes read should match bytes written for stream starting at block {startIndex}.");
                        Assert.That(buffer, Is.EqualTo(expectedData), $"Data mismatch for stream starting at block {startIndex}.");
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                });

                // Assert
                if (!exceptions.IsEmpty)
                {
                    throw new AggregateException(exceptions);
                }
            }
            finally
            {
                // Cleanup: Dispose all BlockStream instances
                foreach (var bs in blockStreams)
                {
                    bs.Dispose();
                }
            }
        }

        /// <summary>
        /// Tests seeking to various positions and performing read/write operations to ensure correct positioning.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void SeekOperations_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);

            // Write data
            var dataToWrite = new byte[blockSize * 2]; // Write two blocks of data
            new Random().NextBytes(dataToWrite);
            long startBlockIndex;
            using (var writeStream = new BlockStream(this.blockManager))
            {
                writeStream.Write(dataToWrite, 0, dataToWrite.Length);
                startBlockIndex = writeStream.StartBlockIndex;
            }

            // Act
            using (var readStream = new BlockStream(this.blockManager, startBlockIndex))
            {
                // Seek to the second block
                readStream.Seek(blockSize, SeekOrigin.Begin);

                var buffer = new byte[blockSize];
                var bytesRead = readStream.Read(buffer, 0, buffer.Length);

                // Assert
                Assert.That(bytesRead, Is.EqualTo(blockSize));
                Assert.That(buffer, Is.EqualTo(dataToWrite.Skip(blockSize).Take(blockSize).ToArray()));
            }
        }

        /// <summary>
        /// Tests setting the length of the stream to a smaller value and ensuring data is truncated correctly.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void SetLength_ShouldTruncateCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 5];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Act: Truncate the stream to blockSize * 3
                var newLength = blockSize * 3;
                blockStream.SetLength(newLength);

                // Assert: Ensure the length is updated
                Assert.That(blockStream.Length, Is.EqualTo(newLength), "Stream length should be truncated correctly.");

                // Verify that data beyond the new length is inaccessible
                blockStream.Seek(newLength, SeekOrigin.Begin);
                var buffer = new byte[blockSize];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(0), "No data should be read beyond the truncated length.");
            }
        }

        /// <summary>
        /// Tests setting the length of the stream to a larger value and ensuring the stream can be extended correctly.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void SetLength_ShouldExtendCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 2];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Act: Extend the stream length
                var newLength = blockSize * 5;
                blockStream.SetLength(newLength);

                // Assert: Ensure the length is updated
                Assert.That(blockStream.Length, Is.EqualTo(newLength), "Stream length should be extended correctly.");

                // Write additional data
                var additionalData = new byte[blockSize * 3];
                new Random().NextBytes(additionalData);
                blockStream.Seek(blockSize * 2, SeekOrigin.Begin);
                blockStream.Write(additionalData, 0, additionalData.Length);

                // Verify the additional data
                blockStream.Seek(blockSize * 2, SeekOrigin.Begin);
                var buffer = new byte[blockSize * 3];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(blockSize * 3), "Bytes read should match bytes written after extension.");
                Assert.That(buffer, Is.EqualTo(additionalData), "Additional data should match written data.");
            }
        }

        /// <summary>
        /// Tests writing data that exactly fills a block and ensures proper block boundary handling.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void EdgeCase_WriteAtBlockBoundary_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize]; // Fill one block's data
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Write another block's data to exactly fill the next block
                var secondData = new byte[blockSize];
                new Random().NextBytes(secondData);
                blockStream.Write(secondData, 0, secondData.Length);

                // Act: Read back the data
                blockStream.Seek(0, SeekOrigin.Begin);
                var buffer1 = new byte[blockSize];
                var bytesRead1 = blockStream.Read(buffer1, 0, buffer1.Length);
                Assert.That(bytesRead1, Is.EqualTo(blockSize), "Bytes read should match bytes written for first block.");
                Assert.That(buffer1, Is.EqualTo(dataToWrite), "First block data mismatch.");

                var buffer2 = new byte[blockSize];
                var bytesRead2 = blockStream.Read(buffer2, 0, buffer2.Length);
                Assert.That(bytesRead2, Is.EqualTo(blockSize), "Bytes read should match bytes written for second block.");
                Assert.That(buffer2, Is.EqualTo(secondData), "Second block data mismatch.");
            }
        }

        /// <summary>
        /// Tests writing and reading zero bytes to ensure no changes occur and no exceptions are thrown.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void EdgeCase_WriteZeroBytes_ShouldNotAffectData(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var initialData = new byte[blockSize];
                new Random().NextBytes(initialData);
                blockStream.Write(initialData, 0, initialData.Length);

                // Act: Write zero bytes
                blockStream.Write(new byte[0], 0, 0);

                // Assert: Data remains unchanged
                blockStream.Seek(0, SeekOrigin.Begin);
                var buffer = new byte[blockSize];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(initialData.Length), "Bytes read should match bytes written.");
                Assert.That(buffer, Is.EqualTo(initialData), "Data should remain unchanged after writing zero bytes.");
            }
        }

        /// <summary>
        /// Tests that multiple BlockStream instances can operate independently without interfering with each other.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void ConcurrentAccess_MultipleStreams_ShouldMaintainDataIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var streamCount = 5;
            var blockStreams = new ConcurrentBag<BlockStream>();
            var dataDictionary = new ConcurrentDictionary<long, byte[]>();

            try
            {
                // Act: Create multiple BlockStream instances and write data
                Parallel.For(0, streamCount, i =>
                {
                    var blockStream = new BlockStream(this.blockManager);
                    blockStreams.Add(blockStream);

                    var dataToWrite = new byte[blockSize * 2];
                    ThreadRandom.Value.NextBytes(dataToWrite);
                    blockStream.Write(dataToWrite, 0, dataToWrite.Length);
                    dataDictionary[blockStream.StartBlockIndex] = dataToWrite;
                });

                // Act: Read back data concurrently
                var exceptions = new ConcurrentBag<Exception>();
                Parallel.ForEach(blockStreams, blockStream =>
                {
                    try
                    {
                        var startIndex = blockStream.StartBlockIndex;
                        var expectedData = dataDictionary[startIndex];
                        var buffer = new byte[expectedData.Length];
                        blockStream.Seek(0, SeekOrigin.Begin);
                        var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                        Assert.That(bytesRead, Is.EqualTo(expectedData.Length), $"Bytes read should match bytes written for stream starting at block {startIndex}.");
                        Assert.That(buffer, Is.EqualTo(expectedData), $"Data mismatch for stream starting at block {startIndex}.");
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                });

                // Assert
                if (!exceptions.IsEmpty)
                {
                    throw new AggregateException(exceptions);
                }
            }
            finally
            {
                // Cleanup: Dispose all BlockStream instances
                foreach (var bs in blockStreams)
                {
                    bs.Dispose();
                }
            }
        }

        /// <summary>
        /// Tests concurrent allocation and deallocation of blocks to ensure the BlockStream maintains integrity.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void MultiThreaded_AllocateAndFreeBlocks_ShouldMaintainIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var totalBlocks = 1000;
            var blockIndices = new ConcurrentBag<long>();
            var exceptions = new ConcurrentBag<Exception>();

            // Act: Allocate blocks concurrently
            Parallel.For(0, totalBlocks, i =>
            {
                try
                {
                    var blockIndex = this.blockManager.AllocateBlock();
                    blockIndices.Add(blockIndex);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });

            // Assert: No exceptions during allocation
            if (!exceptions.IsEmpty)
            {
                throw new AggregateException(exceptions);
            }

            // Act: Free blocks concurrently
            Parallel.ForEach(blockIndices, blockIndex =>
            {
                try
                {
                    this.blockManager.FreeBlock(blockIndex);
                }
                catch (Exception ex)
                {
                    exceptions.Add(ex);
                }
            });

            // Assert: No exceptions during deallocation
            if (!exceptions.IsEmpty)
            {
                throw new AggregateException(exceptions);
            }

            // Verify that freed blocks cannot be freed again
            Parallel.ForEach(blockIndices, blockIndex =>
            {
                Assert.Throws<InvalidOperationException>(() => this.blockManager.FreeBlock(blockIndex),
                    $"Freeing already freed block {blockIndex} should throw an exception.");
            });
        }

        /// <summary>
        /// Tests reading beyond the end of the stream to ensure no data is returned and no exceptions are thrown.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void EdgeCase_ReadBeyondEndOfStream_ShouldReturnZeroBytes(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 3];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Act: Seek to the end and attempt to read
                blockStream.Seek(blockSize * 3, SeekOrigin.Begin);
                var buffer = new byte[blockSize];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);

                // Assert
                Assert.That(bytesRead, Is.EqualTo(0), "Reading beyond the end of the stream should return zero bytes.");
            }
        }

        /// <summary>
        /// Tests that writing data beyond the current length extends the stream correctly.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void WriteBeyondCurrentLength_ShouldExtendStreamCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);

            var initialData = new byte[blockSize * 2];
            new Random().NextBytes(initialData);

            var additionalData = new byte[blockSize * 3];
            new Random().NextBytes(additionalData);

            long startBlockIndex;
            using (var writeStream = new BlockStream(this.blockManager))
            {
                // Write initial data
                writeStream.Write(initialData, 0, initialData.Length);

                // Seek beyond current length
                writeStream.Seek(blockSize * 5, SeekOrigin.Begin);

                // Write additional data
                writeStream.Write(additionalData, 0, additionalData.Length);
                startBlockIndex = writeStream.StartBlockIndex;
            }

            // Act
            using (var readStream = new BlockStream(this.blockManager, startBlockIndex))
            {
                // Read the data beyond the initial length
                readStream.Seek(blockSize * 5, SeekOrigin.Begin);
                var buffer = new byte[additionalData.Length];
                var bytesRead = readStream.Read(buffer, 0, buffer.Length);

                // Assert
                Assert.That(bytesRead, Is.EqualTo(additionalData.Length), "Bytes read should match bytes written beyond current length.");
                Assert.That(buffer, Is.EqualTo(additionalData));
            }
        }

        /// <summary>
        /// Tests that disposing the BlockStream properly flushes and releases resources without data loss.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void Dispose_ShouldFlushDataCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var dataToWrite = new byte[blockSize * 2];
            new Random().NextBytes(dataToWrite);

            // Act
            using (var blockStream = new BlockStream(this.blockManager))
            {
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);
                // Dispose is called implicitly by the using statement
            }

            // Assert: Re-open the BlockStream and verify data
            using (var blockStream = new BlockStream(this.blockManager, 0))
            {
                var buffer = new byte[blockSize * 2];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(dataToWrite.Length), "Bytes read should match bytes written before dispose.");
                Assert.That(buffer, Is.EqualTo(dataToWrite), "Data should match after dispose and re-opening the stream.");
            }
        }

        /// <summary>
        /// Tests writing and reading using a MemoryStream to ensure functionality without actual file I/O.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void MemoryStream_ReadWriteBlocks_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var blockCount = 100;
            var dataDictionary = new Dictionary<long, byte[]>();
            var random = new Random(); // Single Random instance

            // Allocate and write data to blocks via individual BlockStreams
            for (var i = 0; i < blockCount; i++)
            {
                var dataToWrite = new byte[blockSize];
                random.NextBytes(dataToWrite);
                using (var blockStream = new BlockStream(this.blockManager))
                {
                    blockStream.Write(dataToWrite, 0, dataToWrite.Length);
                    blockStream.Flush(); // Ensure data is flushed
                    dataDictionary[blockStream.StartBlockIndex] = dataToWrite;
                }
            }

            // Act & Assert: Read back the data and verify
            foreach (var kvp in dataDictionary)
            {
                using (var readStream = new BlockStream(this.blockManager, kvp.Key))
                {
                    var buffer = new byte[kvp.Value.Length];
                    var bytesRead = readStream.Read(buffer, 0, buffer.Length);
                    Assert.That(bytesRead, Is.EqualTo(kvp.Value.Length), $"Bytes read should match bytes written for block {kvp.Key}.");
                    Assert.That(buffer, Is.EqualTo(kvp.Value), $"Data mismatch for block {kvp.Key}.");
                }
            }
        }

        /// <summary>
        /// Tests reading and writing large amounts of data to ensure scalability and data integrity.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(4096)]
        public void LargeData_ReadWrite_ShouldMaintainDataIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataSize = blockSize * 1000; // 1000 blocks
                var dataToWrite = new byte[dataSize];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Act: Read back the data in chunks
                blockStream.Seek(0, SeekOrigin.Begin);
                var buffer = new byte[blockSize * 100];
                var totalBytesRead = 0;
                while (totalBytesRead < dataSize)
                {
                    var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                    if (bytesRead == 0)
                        break;

                    Assert.That(buffer.Take(bytesRead).ToArray(), Is.EqualTo(dataToWrite.Skip(totalBytesRead).Take(bytesRead).ToArray()),
                        $"Data mismatch at byte position {totalBytesRead}.");
                    totalBytesRead += bytesRead;
                }

                // Assert
                Assert.That(totalBytesRead, Is.EqualTo(dataSize), "Total bytes read should match total bytes written.");
            }
        }

        /// <summary>
        /// Tests that attempting to seek to a negative position throws an IOException.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void Seek_ToNegativePosition_ShouldThrowIOException(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                // Act & Assert
                var ex = Assert.Throws<IOException>(() => blockStream.Seek(-10, SeekOrigin.Begin));
                Assert.That(ex.Message, Is.EqualTo("Cannot seek to a negative position."));
            }
        }

        /// <summary>
        /// Tests that writing data with invalid buffer parameters throws appropriate exceptions.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void Write_WithInvalidBufferParameters_ShouldThrowException(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var invalidBuffer = new byte[blockSize];
                var validData = new byte[blockSize];
                new Random().NextBytes(validData);
                blockStream.Write(validData, 0, validData.Length);

                // Act & Assert: Negative offset
                var ex1 = Assert.Throws<ArgumentOutOfRangeException>(() => blockStream.Write(invalidBuffer, -1, 10));
                Assert.That(ex1.ParamName, Is.EqualTo("offset"));
            }
        }

        /// <summary>
        /// Tests that reading data with invalid buffer parameters throws appropriate exceptions.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void Read_WithInvalidBufferParameters_ShouldThrowException(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var validData = new byte[blockSize];
                new Random().NextBytes(validData);
                blockStream.Write(validData, 0, validData.Length);
                blockStream.Seek(0, SeekOrigin.Begin);

                var invalidBuffer = new byte[blockSize];

                // Act & Assert: Negative offset
                var ex1 = Assert.Throws<ArgumentOutOfRangeException>(() => blockStream.Read(invalidBuffer, -1, 10));
                Assert.That(ex1.ParamName, Is.EqualTo("offset"));

                // Act & Assert: Negative count
                var ex2 = Assert.Throws<ArgumentOutOfRangeException>(() => blockStream.Read(invalidBuffer, 0, -5));
                Assert.That(ex2.ParamName, Is.EqualTo("count"));
            }
        }

        /// <summary>
        /// Tests that attempting to set a negative length throws an ArgumentOutOfRangeException.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void SetLength_ToNegativeValue_ShouldThrowArgumentOutOfRangeException(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                // Act & Assert
                var ex = Assert.Throws<ArgumentOutOfRangeException>(() => blockStream.SetLength(-100));
                Assert.That(ex.ParamName, Is.EqualTo("value"));
                Assert.That(ex.Message, Does.Contain("Length cannot be negative."));
            }
        }

        /// <summary>
        /// Tests that multiple BlockStream instances can read from the same starting block without data corruption.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void MultipleStreams_ReadFromSameStartBlock_ShouldMaintainDataIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var mainStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 2];
                new Random().NextBytes(dataToWrite);
                mainStream.Write(dataToWrite, 0, dataToWrite.Length);
                var startBlock = mainStream.StartBlockIndex;

                // Act: Create multiple streams pointing to the same start block
                var exceptions = new ConcurrentBag<Exception>();
                Parallel.For(0, 5, i =>
                {
                    try
                    {
                        using (var readStream = new BlockStream(this.blockManager, startBlock))
                        {
                            var buffer = new byte[dataToWrite.Length];
                            var bytesRead = readStream.Read(buffer, 0, buffer.Length);
                            Assert.That(bytesRead, Is.EqualTo(dataToWrite.Length), $"Bytes read should match bytes written for stream {i}.");
                            Assert.That(buffer, Is.EqualTo(dataToWrite), $"Data mismatch for stream {i}.");
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                });

                // Assert
                if (!exceptions.IsEmpty)
                {
                    throw new AggregateException(exceptions);
                }
            }
        }

        /// <summary>
        /// Tests that disposing a BlockStream does not affect other streams using the same BlockManager.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void Dispose_OneStream_ShouldNotAffectOthers(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite, FileShare.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);

            BlockStream stream1 = null;
            BlockStream stream2 = null;

            try
            {
                stream1 = new BlockStream(this.blockManager);
                stream2 = new BlockStream(this.blockManager);

                var data1 = new byte[blockSize];
                var data2 = new byte[blockSize];
                new Random().NextBytes(data1);
                new Random().NextBytes(data2);

                stream1.Write(data1, 0, data1.Length);
                stream2.Write(data2, 0, data2.Length);

                // Act: Dispose stream1
                stream1.Dispose();

                // Assert: stream2 should still be able to read its data correctly
                stream2.Seek(0, SeekOrigin.Begin);
                var buffer = new byte[data2.Length];
                var bytesRead = stream2.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(data2.Length), "Bytes read should match bytes written for stream2.");
                Assert.That(buffer, Is.EqualTo(data2), "Data read should match data written for stream2.");
            }
            finally
            {
                stream1?.Dispose();
                stream2?.Dispose();
            }
        }

        /// <summary>
        /// Tests that after setting the stream length to zero, all blocks are freed and AllocatedBlocks is zero.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void SetLengthToZero_ShouldFreeAllBlocks(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 5];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Act: Set length to zero
                blockStream.SetLength(0);

                // Assert: Ensure the length is zero
                Assert.That(blockStream.Length, Is.EqualTo(0), "Stream length should be zero after truncation.");

                // Assert: Ensure all blocks are freed
                Assert.That(this.blockManager.AllocatedBlocks, Is.EqualTo(0), "All blocks should be freed after setting length to zero.");
            }
        }

        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void MultiThreaded_NonOverlappingReadWrite_ShouldMaintainDataIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var totalThreads = 50;
            var dataSizePerThread = blockSize * 2;
            var totalDataSize = dataSizePerThread * totalThreads;
            var data = new byte[totalDataSize];
            new Random().NextBytes(data);
            long startBlockIndex;

            using (var blockStream = new BlockStream(this.blockManager))
            {
                // Pre-fill the stream with data
                blockStream.Write(data, 0, data.Length);
                startBlockIndex = blockStream.StartBlockIndex;
            }

            var exceptions = new ConcurrentBag<Exception>();
            var tasks = new List<Task>();

            // Act: Each thread operates on its own data segment
            for (var i = 0; i < totalThreads; i++)
            {
                var threadIndex = i;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        using (var localStream = new BlockStream(this.blockManager, startBlockIndex))
                        {
                            var rnd = ThreadRandom.Value;
                            var position = threadIndex * dataSizePerThread;
                            var buffer = new byte[dataSizePerThread];

                            // Randomly decide to read or write
                            if (rnd.Next(2) == 0)
                            {
                                // Read
                                localStream.Seek(position, SeekOrigin.Begin);
                                var bytesRead = localStream.Read(buffer, 0, buffer.Length);

                                // Verify data
                                lock (data)
                                {
                                    var expectedData = data.Skip(position).Take(bytesRead).ToArray();
                                    Assert.That(buffer, Is.EqualTo(expectedData), $"Data mismatch at position {position}.");
                                }
                            }
                            else
                            {
                                // Write
                                rnd.NextBytes(buffer);

                                localStream.Seek(position, SeekOrigin.Begin);
                                localStream.Write(buffer, 0, buffer.Length);

                                // Update the original data array
                                lock (data)
                                {
                                    Array.Copy(buffer, 0, data, position, buffer.Length);
                                }
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        exceptions.Add(ex);
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray());

            // Assert
            if (!exceptions.IsEmpty)
            {
                throw new AggregateException(exceptions);
            }

            // Verify the entire data
            using (var verificationStream = new BlockStream(this.blockManager, startBlockIndex))
            {
                var buffer = new byte[totalDataSize];
                verificationStream.Read(buffer, 0, buffer.Length);
                Assert.That(buffer, Is.EqualTo(data), "Final data mismatch after concurrent operations.");
            }
        }

        /// <summary>
        /// Tests that the Position property accurately reflects the current position after various read/write/seek operations.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void PositionProperty_ShouldReflectCurrentPosition(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 3];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Act & Assert: Check position after write
                Assert.That(blockStream.Position, Is.EqualTo(dataToWrite.Length), "Position should be at the end after write.");

                // Seek to beginning
                blockStream.Seek(0, SeekOrigin.Begin);
                Assert.That(blockStream.Position, Is.EqualTo(0), "Position should be zero after seeking to the beginning.");

                // Read some data
                var buffer = new byte[blockSize];
                blockStream.Read(buffer, 0, buffer.Length);
                Assert.That(blockStream.Position, Is.EqualTo(blockSize), "Position should advance by the number of bytes read.");

                // Seek relative to current position
                blockStream.Seek(blockSize, SeekOrigin.Current);
                Assert.That(blockStream.Position, Is.EqualTo(blockSize * 2), "Position should reflect seek from current position.");

                // Seek from end
                blockStream.Seek(-blockSize, SeekOrigin.End);
                Assert.That(blockStream.Position, Is.EqualTo(dataToWrite.Length - blockSize), "Position should reflect seek from end.");
            }
        }

        /// <summary>
        /// Tests that the BlockStream reports the correct capabilities for reading, writing, and seeking.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void StreamCapabilities_ShouldBeCorrect(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);

            // Act
            using (var blockStream = new BlockStream(this.blockManager))
            {
                // Assert
                Assert.That(blockStream.CanRead, Is.True, "BlockStream should support reading.");
                Assert.That(blockStream.CanWrite, Is.True, "BlockStream should support writing.");
                Assert.That(blockStream.CanSeek, Is.True, "BlockStream should support seeking.");
                Assert.That(blockStream.CanTimeout, Is.False, "BlockStream should not support timeouts by default.");
            }
        }

        /// <summary>
        /// Tests that operations on a closed BlockStream throw ObjectDisposedException.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void OperationsAfterDispose_ShouldThrowObjectDisposedException(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            var blockStream = new BlockStream(this.blockManager);
            var dataToWrite = new byte[blockSize];
            new Random().NextBytes(dataToWrite);
            blockStream.Write(dataToWrite, 0, dataToWrite.Length);

            // Act: Dispose the BlockStream
            blockStream.Dispose();

            // Assert: Operations should throw ObjectDisposedException
            Assert.Throws<ObjectDisposedException>(() => blockStream.Write(dataToWrite, 0, dataToWrite.Length), "Writing after disposing BlockStream should throw.");
            Assert.Throws<ObjectDisposedException>(() => blockStream.Read(dataToWrite, 0, dataToWrite.Length), "Reading after disposing BlockStream should throw.");
            Assert.Throws<ObjectDisposedException>(() => blockStream.Seek(0, SeekOrigin.Begin), "Seeking after disposing BlockStream should throw.");
        }

        /// <summary>
        /// Tests that passing a null buffer to Read or Write methods throws an ArgumentNullException.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void NullBuffer_ShouldThrowArgumentNullException(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                blockStream.Seek(0, SeekOrigin.Begin);

                // Act & Assert: Write with null buffer
                Assert.Throws<ArgumentNullException>(() => blockStream.Write(null, 0, 10), "Writing with null buffer should throw ArgumentNullException.");

                // Act & Assert: Read with null buffer
                Assert.Throws<ArgumentNullException>(() => blockStream.Read(null, 0, 10), "Reading with null buffer should throw ArgumentNullException.");
            }
        }

        /// <summary>
        /// Tests that the BlockManager's AllocatedBlocks count reflects the correct number of allocated blocks after operations.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void AllocatedBlocksCount_ShouldBeAccurate(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);

            // Initial count should be zero
            Assert.That(this.blockManager.AllocatedBlocks, Is.EqualTo(0), "Initial AllocatedBlocks should be zero.");

            // Act: Allocate blocks via BlockStream
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 3];
                new Random().NextBytes(dataToWrite);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Assert: AllocatedBlocks should reflect the number of blocks used
                var expectedBlocks = (int)Math.Ceiling((double)dataToWrite.Length / blockSize);
                Assert.That(this.blockManager.AllocatedBlocks, Is.EqualTo(expectedBlocks), $"AllocatedBlocks should be {expectedBlocks} after writing data.");
            }

            // After disposing the BlockStream, blocks should still be allocated
            Assert.That(this.blockManager.AllocatedBlocks, Is.EqualTo(3), "AllocatedBlocks should remain after disposing the stream.");
        }

        /// <summary>
        /// Tests that calling Flush on the BlockStream flushes data to the underlying stream.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void Flush_ShouldWriteDataToUnderlyingStream(int blockSize)
        {
            // Arrange
            var underlyingStream = new MemoryStream();
            this.stream = underlyingStream;
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var dataToWrite = new byte[blockSize * 2];
                new Random().NextBytes(dataToWrite);

                // Act: Write data and flush
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);
                blockStream.Flush();

                // Assert: Underlying stream length should reflect the written data
                Assert.That(underlyingStream.Length, Is.GreaterThan(0), "Underlying stream should have data after flush.");
            }
        }

        /// <summary>
        /// Tests that setting the length larger than current length extends the stream, and reading from the extended area returns zeros.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void SetLength_LargerThanCurrentLength_ShouldExtendWithZeros(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                var initialData = new byte[blockSize];
                new Random().NextBytes(initialData);
                blockStream.Write(initialData, 0, initialData.Length);

                // Act: Extend the length without writing new data
                var newLength = blockSize * 3;
                blockStream.SetLength(newLength);

                // Assert: Length is updated
                Assert.That(blockStream.Length, Is.EqualTo(newLength), "Stream length should be extended correctly.");

                // Read from the extended area
                blockStream.Seek(blockSize, SeekOrigin.Begin);
                var buffer = new byte[blockSize * 2];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);

                // Assert: Extended area should return zeros
                var expectedBuffer = new byte[buffer.Length]; // zeros
                Assert.That(bytesRead, Is.EqualTo(buffer.Length), "Bytes read should match the extended length.");
                Assert.That(buffer, Is.EqualTo(expectedBuffer), "Extended area should be filled with zeros.");
            }
        }

        /// <summary>
        /// Tests that multiple sequential calls to SetLength increase and decrease the stream length correctly.
        /// </summary>
        /// <param name="blockSize">The size of each block.</param>
        [Test]
        [TestCase(1)]
        [TestCase(512)]
        [TestCase(4096)]
        public void MultipleSetLength_Calls_ShouldAdjustLengthCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize + BlockStream.MetadataLength);
            using (var blockStream = new BlockStream(this.blockManager))
            {
                // Initial length is zero
                Assert.That(blockStream.Length, Is.EqualTo(0), "Initial length should be zero.");

                // Act: Increase length
                blockStream.SetLength(blockSize * 4);
                Assert.That(blockStream.Length, Is.EqualTo(blockSize * 4), "Length should be increased to 4 blocks.");

                // Decrease length
                blockStream.SetLength(blockSize * 2);
                Assert.That(blockStream.Length, Is.EqualTo(blockSize * 2), "Length should be decreased to 2 blocks.");

                // Increase length again
                blockStream.SetLength(blockSize * 5);
                Assert.That(blockStream.Length, Is.EqualTo(blockSize * 5), "Length should be increased to 5 blocks.");

                // Write data to check integrity
                var dataToWrite = new byte[blockSize * 5];
                new Random().NextBytes(dataToWrite);
                blockStream.Seek(0, SeekOrigin.Begin);
                blockStream.Write(dataToWrite, 0, dataToWrite.Length);

                // Assert: Read back data
                blockStream.Seek(0, SeekOrigin.Begin);
                var buffer = new byte[dataToWrite.Length];
                var bytesRead = blockStream.Read(buffer, 0, buffer.Length);
                Assert.That(bytesRead, Is.EqualTo(dataToWrite.Length), "Bytes read should match bytes written after multiple SetLength calls.");
                Assert.That(buffer, Is.EqualTo(dataToWrite), "Data read should match data written.");
            }
        }
    }
}