<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;

abstract class AbstractInsertBatchTest extends AbstractBatchFunctionalTest
{
    public function testWriteConcernError()
    {
        $this->requiresReplicaSet();

        $batch = $this->getBatch(array('w' => 99, 'wTimeoutMS' => 1));
        $batch->add(array('_id' => 1, 'x' => 1));
        $batch->add(array('_id' => 2, 'x' => 2));
        $result = $this->executeBatch($batch);

        $this->assertSame(2, $result['nInserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertNumWriteConcernErrors(1, $result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
        ));
    }

    public function testWriteErrorWithOrderedBatch()
    {
        $batch = $this->getBatch(array('ordered' => true));
        $batch->add(array('_id' => 1, 'x' => 1));
        $batch->add(array('_id' => 1, 'x' => 2));
        $batch->add(array('_id' => 2, 'x' => 3));
        $result = $this->executeBatch($batch);

        $this->assertSame(1, $result['nInserted']);
        $this->assertNumWriteErrors(1, $result);
        $this->assertNumWriteConcernErrors(0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
        ));
    }

    public function testWriteErrorWithUnorderedBatch()
    {
        $batch = $this->getBatch(array('ordered' => false));
        $batch->add(array('_id' => 1, 'x' => 1));
        $batch->add(array('_id' => 1, 'x' => 2));
        $batch->add(array('_id' => 2, 'x' => 3));
        $batch->add(array('_id' => 2, 'x' => 4));
        $result = $this->executeBatch($batch);

        $this->assertSame(2, $result['nInserted']);
        $this->assertNumWriteErrors(2, $result);
        $this->assertNumWriteConcernErrors(0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 3),
        ));
    }

    public function testSingleInsertOperation()
    {
        $batch = $this->getBatch();
        $batch->add(array('_id' => 1, 'x' => 1));
        $result = $this->executeBatch($batch);

        $this->assertSame(1, $result['nInserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertNumWriteConcernErrors(0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
        ));
    }

    public function testMultipleInsertOperations()
    {
        $batch = $this->getBatch();
        $batch->add(array('_id' => 1, 'x' => 1));
        $batch->add(array('_id' => 2, 'x' => 2));
        $batch->add(array('_id' => 3, 'x' => 3));
        $result = $this->executeBatch($batch);

        $this->assertSame(3, $result['nInserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertNumWriteConcernErrors(0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
            array('_id' => 3, 'x' => 3),
        ));
    }

    public function testGetItemCount()
    {
        $batch = $this->getBatch();
        $operation = array('foo' => 1);

        $this->assertSame(0, $batch->getItemCount());
        $batch->add($operation);
        $this->assertSame(1, $batch->getItemCount());
        $batch->add($operation);
        $this->assertSame(2, $batch->getItemCount());
        $batch->add($operation);
        $this->assertSame(3, $batch->getItemCount());
    }

    public function testGetType()
    {
        $batch = $this->getBatch();

        $this->assertSame(BatchInterface::OP_INSERT, $batch->getType());
    }
}
