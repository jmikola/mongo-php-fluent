<?php

namespace MongoDB\Tests;

use MongoDB\Batch\MappedBatch;
use MongoDB\Batch\BatchInterface;
use MongoDB\Tests\BaseTestCase;
use BadMethodCallException;

class MappedBatchTest extends BaseTestCase
{
    public function testAddShouldInvokeBatchMethod()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $batch->expects($this->once())
            ->method('add')
            ->with(array('_id' => 1));

        $mappedBatch->add(array('_id' => 1), 0);
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage Bulk index cannot be negative: -1
     */
    public function testAddShouldNotAllowNegativeBulkIndex()
    {
        $mappedBatch = new MappedBatch($this->getMockBatch());
        $mappedBatch->add(array('_id' => 1), -1);
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage Document already exists for bulk index: 0
     */
    public function testAddShouldNotAllowDuplicateBulkIndexes()
    {
        $mappedBatch = new MappedBatch($this->getMockBatch());
        $mappedBatch->add(array('_id' => 1), 0);
        $mappedBatch->add(array('_id' => 1), 0);
    }

    /**
     * @expectedException OutOfBoundsException
     * @expectedExceptionMessage No document exists for batch index: 0
     */
    public function testAddShouldNotUpdateIndexMapUnlessAddingToBatchSucceeds()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $batch->expects($this->once())
            ->method('add')
            ->will($this->throwException(new BadMethodCallException()));

        try {
            $mappedBatch->add(array('_id' => 1), 0);
            $this->fail('Expected BadMethodCallException to be thrown');
        } catch (BadMethodCallException $e) {}

        $mappedBatch->getBulkIndex(0);
    }

    public function testAddShouldUpdateIndexMapAfterAddingToBatch()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $mappedBatch->add(array('_id' => 1), 5);
        $this->assertSame(5, $mappedBatch->getBulkIndex(0));
    }

    public function testExecuteShouldProxyBatchMethod()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $batch->expects($this->once())
            ->method('getItemCount')
            ->will($this->returnValue(1));

        $batch->expects($this->once())
            ->method('execute')
            ->with(array('w' => 1));

        $mappedBatch->execute(array('w' => 1));
    }

    /**
     * @expectedException MongoException
     * @expectedExceptionMessage Cannot call execute() for an empty batch
     */
    public function testExecuteShouldThrowExceptionForEmptyBatch()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $batch->expects($this->once())
            ->method('getItemCount')
            ->will($this->returnValue(0));

        $mappedBatch->execute();
    }

    public function testGetItemCountShouldProxyBatchMethod()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $batch->expects($this->once())
            ->method('getItemCount')
            ->will($this->returnValue(5));

        $this->assertSame(5, $mappedBatch->getItemCount());
    }

    public function testGetTypeShouldProxyBatchMethod()
    {
        $batch = $this->getMockBatch();
        $mappedBatch = new MappedBatch($batch);

        $batch->expects($this->once())
            ->method('getType')
            ->will($this->returnValue(BatchInterface::OP_INSERT));

        $this->assertSame(BatchInterface::OP_INSERT, $mappedBatch->getType());
    }

    protected function getMockBatch()
    {
        return $this->getMock('MongoDB\Batch\BatchInterface');
    }
}
