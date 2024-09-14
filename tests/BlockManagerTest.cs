using System.Collections.Concurrent;

namespace Itexoft.Virtual.IO.Tests
{
    [TestFixture]
    public class BlockManagerTest
    {
        private Stream stream;
        private BlockManager blockManager;

        [SetUp]
        public void Setup()
        {
            // The stream will be initialized in each test with the appropriate block size
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

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void SingleThreaded_ReadWriteBlocks_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockCount = 100;
            var dataDictionary = new Dictionary<long, byte[]>();

            // Allocate and write data to blocks
            for (var i = 0; i < blockCount; i++)
            {
                var blockIndex = this.blockManager.AllocateBlock();
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);

                this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
                dataDictionary[blockIndex] = dataToWrite;
            }

            // Act & Assert
            foreach (var kvp in dataDictionary)
            {
                var buffer = new byte[blockSize];
                this.blockManager.ReadBlock(kvp.Key, 0, buffer, 0, buffer.Length);
                Assert.That(buffer, Is.EqualTo(kvp.Value).AsCollection, $"Data mismatch in block {kvp.Key}");
            }
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void Multithreaded_ReadWriteBlocks_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockCount = 1000;
            var blockIndices = new List<long>();
            var dataDictionary = new ConcurrentDictionary<long, byte[]>();
            var exceptions = new ConcurrentBag<Exception>();

            // Allocate blocks
            for (var i = 0; i < blockCount; i++)
            {
                var blockIndex = this.blockManager.AllocateBlock();
                blockIndices.Add(blockIndex);
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);
                dataDictionary[blockIndex] = dataToWrite;
            }

            // Act
            Parallel.ForEach(blockIndices, blockIndex =>
            {
                try
                {
                    // Write data
                    var dataToWrite = dataDictionary[blockIndex];
                    this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);

                    // Read data
                    var buffer = new byte[blockSize];
                    this.blockManager.ReadBlock(blockIndex, 0, buffer, 0, buffer.Length);

                    // Verify data
                    Assert.That(buffer, Is.EqualTo(dataToWrite).AsCollection, $"Data mismatch in block {blockIndex}");
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

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void EdgeCase_BlockBoundaryReadWrite_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockIndex = this.blockManager.AllocateBlock();
            var dataToWrite = new byte[blockSize];

            // Fill data with a pattern
            for (var i = 0; i < dataToWrite.Length; i++)
            {
                dataToWrite[i] = (byte)(i % 256);
            }

            // Act
            this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
            var buffer = new byte[blockSize];
            this.blockManager.ReadBlock(blockIndex, 0, buffer, 0, buffer.Length);

            // Assert
            Assert.That(buffer, Is.EqualTo(dataToWrite).AsCollection, $"Data mismatch in block {blockIndex}");
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void EdgeCase_FreeUnallocatedBlock_ShouldThrowException(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            // Act & Assert
            var ex = Assert.Throws<InvalidOperationException>(() => this.blockManager.FreeBlock(0));
            Assert.That(ex.Message, Contains.Substring("Block is already free."));
        }

        //[Test]
        //[TestCase(17)]
        //[TestCase(512)]
        //[TestCase(65536)]
        //public void EdgeCase_ReadUnallocatedBlock_ShouldThrowException(int blockSize)
        //{
        //    // Arrange
        //    this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
        //    this.blockManager = new BlockManager(this.stream, blockSize);

        //    var buffer = new byte[blockSize];

        //    // Act & Assert
        //    var ex = Assert.Throws<InvalidOperationException>(() => this.blockManager.ReadBlock(0, 0, buffer, 0, buffer.Length));
        //    Assert.That(ex.Message, Is.EqualTo("Block is not allocated."));
        //}

        //[Test]
        //[TestCase(17)]
        //[TestCase(512)]
        //[TestCase(65536)]
        //public void EdgeCase_WriteUnallocatedBlock_ShouldThrowException(int blockSize)
        //{
        //    // Arrange
        //    this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
        //    this.blockManager = new BlockManager(this.stream, blockSize);

        //    var dataToWrite = new byte[blockSize];

        //    // Act & Assert
        //    var ex = Assert.Throws<InvalidOperationException>(() => this.blockManager.WriteBlock(0, 0, dataToWrite, 0, dataToWrite.Length));
        //    Assert.That(ex.Message, Is.EqualTo("Block is not allocated."));
        //}

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void EdgeCase_WriteBeyondBlockSize_ShouldThrowException(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockIndex = this.blockManager.AllocateBlock();
            var dataToWrite = new byte[blockSize + 1];

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length));
            Assert.That(ex.Message, Is.EqualTo("Data length exceeds block size."));
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void EdgeCase_ReadBeyondBlockSize_ShouldThrowException(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockIndex = this.blockManager.AllocateBlock();
            var buffer = new byte[blockSize + 1];

            // Act & Assert
            var ex = Assert.Throws<ArgumentException>(() => this.blockManager.ReadBlock(blockIndex, 0, buffer, 0, buffer.Length));
            Assert.That(ex.Message, Is.EqualTo("Data length exceeds block size."));
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        public void MemoryStream_ReadWriteBlocks_ShouldWorkCorrectly(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockCount = 100;
            var dataDictionary = new Dictionary<long, byte[]>();

            // Allocate and write data to blocks
            for (var i = 0; i < blockCount; i++)
            {
                var blockIndex = this.blockManager.AllocateBlock();
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);

                this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
                dataDictionary[blockIndex] = dataToWrite;
            }

            // Act & Assert
            foreach (var kvp in dataDictionary)
            {
                var buffer = new byte[blockSize];
                this.blockManager.ReadBlock(kvp.Key, 0, buffer, 0, buffer.Length);
                Assert.That(buffer, Is.EqualTo(kvp.Value).AsCollection, $"Data mismatch in block {kvp.Key}");
            }
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        [TestCase(65536)]
        public void Concurrent_AllocateAndFreeBlocks_ShouldMaintainIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var totalBlocks = 1000;
            var blockIndices = new ConcurrentBag<long>();
            var exceptions = new ConcurrentBag<Exception>();

            // Act
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

            if (!exceptions.IsEmpty)
            {
                throw new AggregateException(exceptions);
            }

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

            // Assert
            if (!exceptions.IsEmpty)
            {
                throw new AggregateException(exceptions);
            }

            foreach (var blockIndex in blockIndices)
            {
                Assert.Throws<InvalidOperationException>(() => this.blockManager.FreeBlock(blockIndex));
            }
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        public void AllocateBlocks_TriggerBatExpansion_DataIntegrityMaintained(int blockSize)
        {
            // Arrange
            this.stream = new FileStream("test.bin", FileMode.Create, FileAccess.ReadWrite);
            this.blockManager = new BlockManager(this.stream, blockSize);

            var initialBlockCount = this.blockManager.TotalBlocks;
            var dataDictionary = new Dictionary<long, byte[]>();

            // Allocate and write data to initial blocks
            for (var i = 0; i < initialBlockCount; i++)
            {
                var blockIndex = this.blockManager.AllocateBlock();
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);

                this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
                dataDictionary[blockIndex] = dataToWrite;
            }

            // Allocate additional blocks to trigger BAT expansion
            var additionalBlockCount = initialBlockCount;
            for (var i = 0; i < additionalBlockCount; i++)
            {
                var blockIndex = this.blockManager.AllocateBlock();
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);

                this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
                dataDictionary[blockIndex] = dataToWrite;
            }

            // Act & Assert
            foreach (var kvp in dataDictionary)
            {
                var buffer = new byte[blockSize];
                this.blockManager.ReadBlock(kvp.Key, 0, buffer, 0, buffer.Length);
                Assert.That(buffer, Is.EqualTo(kvp.Value).AsCollection, $"Data mismatch in block {kvp.Key}");
            }
        }

        [Test]
        [TestCase(17)]
        [TestCase(512)]
        public void AllocateAndFreeBlocks_MemoryStream_ShouldMaintainIntegrity(int blockSize)
        {
            // Arrange
            this.stream = new MemoryStream();
            this.blockManager = new BlockManager(this.stream, blockSize);

            var blockCount = 500;
            var dataDictionary = new ConcurrentDictionary<long, byte[]>();

            // Act
            Parallel.For(0, blockCount, i =>
            {
                var blockIndex = this.blockManager.AllocateBlock();
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);
                this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
                dataDictionary[blockIndex] = dataToWrite;
            });

            // Free half of the blocks
            var blocksToFree = dataDictionary.Keys.Take(blockCount / 2).ToList();
            Parallel.ForEach(blocksToFree, blockIndex =>
            {
                this.blockManager.FreeBlock(blockIndex);
            });

            // Allocate additional blocks
            var additionalBlockCount = blockCount / 2;
            Parallel.For(0, additionalBlockCount, i =>
            {
                var blockIndex = this.blockManager.AllocateBlock();
                var dataToWrite = new byte[blockSize];
                new Random().NextBytes(dataToWrite);
                this.blockManager.WriteBlock(blockIndex, 0, dataToWrite, 0, dataToWrite.Length);
                dataDictionary[blockIndex] = dataToWrite;
            });

            // Assert
            foreach (var kvp in dataDictionary)
            {
                var buffer = new byte[blockSize];
                this.blockManager.ReadBlock(kvp.Key, 0, buffer, 0, buffer.Length);
                Assert.That(buffer, Is.EqualTo(kvp.Value).AsCollection, $"Data mismatch in block {kvp.Key}");
            }
        }
    }
}