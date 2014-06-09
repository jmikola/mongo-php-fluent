<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;
use MongoDB\Tests\BaseTestCase;

abstract class AbstractGeneratorTest extends BaseTestCase
{
    public function testGeneratorShouldImplementIterator()
    {
        $generator = $this->getGenerator();
        $this->assertInstanceOf('Iterator', $generator);
    }

    public function testEmptyOperationsShouldGenerateNoBatches()
    {
        $generator = $this->getGenerator();
        $this->assertEmpty(iterator_to_array($generator));
    }

    public function testOperationsOfTheSameTypeShouldGenerateOneBatch()
    {
        $operations = array(
            array(BatchInterface::OP_INSERT, array('_id' => 1)),
            array(BatchInterface::OP_INSERT, array('_id' => 2)),
            array(BatchInterface::OP_INSERT, array('_id' => 3)),
        );

        $generator = $this->getGenerator($operations);
        $batches = iterator_to_array($generator);

        $this->assertCount(1, $batches);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_INSERT, 3, $batches[0]);
    }

    protected function assertMappedBatchTypeAndItemCount($expectedType, $expectedItemCount, $batch)
    {
        $this->assertInstanceOf('MongoDB\Batch\MappedBatch', $batch);
        $this->assertSame($expectedType, $batch->getType());
        $this->assertSame($expectedItemCount, $batch->getItemCount());
    }

    abstract protected function getGenerator(array $operations = array(), array $writeOptions = array());
}
