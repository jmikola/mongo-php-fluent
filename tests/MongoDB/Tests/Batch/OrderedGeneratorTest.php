<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;
use MongoDB\Batch\OrderedGenerator;
use MongoDB\Stubs\MongoClient;

class OrderedGeneratorTest extends AbstractGeneratorTest
{
    public function testBatchesShouldBeGeneratedEachTimeTheOperationTypeChanges()
    {
        $operations = array(
            array(BatchInterface::OP_INSERT, array('_id' => 1)),
            array(BatchInterface::OP_INSERT, array('_id' => 2)),
            array(BatchInterface::OP_UPDATE, array('q' => array('_id' => 1), 'u' => array('foo' => 'bar'))),
            array(BatchInterface::OP_UPDATE, array('q' => array('_id' => 2), 'u' => array('foo' => 'bar'))),
            array(BatchInterface::OP_DELETE, array('q' => array('_id' => 1), 'limit' => 1)),
            array(BatchInterface::OP_DELETE, array('q' => array('_id' => 2), 'limit' => 1)),
            array(BatchInterface::OP_INSERT, array('_id' => 3)),
            array(BatchInterface::OP_UPDATE, array('q' => array('_id' => 3), 'u' => array('foo' => 'bar'))),
            array(BatchInterface::OP_DELETE, array('q' => array('_id' => 3), 'limit' => 1)),
        );

        $generator = $this->getGenerator($operations);
        $batches = iterator_to_array($generator);

        $this->assertCount(6, $batches);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_INSERT, 2, $batches[0]);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_UPDATE, 2, $batches[1]);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_DELETE, 2, $batches[2]);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_INSERT, 1, $batches[3]);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_UPDATE, 1, $batches[4]);
        $this->assertMappedBatchTypeAndItemCount(BatchInterface::OP_DELETE, 1, $batches[5]);
    }

    /**
     * @see AbstractGeneratorTest::getGenerator()
     */
    protected function getGenerator(array $operations = array(), array $writeOptions = array())
    {
        return new OrderedGenerator($this->getMockMongoDB(), $this->getMockMongoCollection(), $operations, $writeOptions);
    }
}
