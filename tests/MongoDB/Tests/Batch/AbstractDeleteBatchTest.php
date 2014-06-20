<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;

abstract class AbstractDeleteBatchTest extends AbstractBatchFunctionalTest
{
    public function testWriteConcernError()
    {
        $this->requiresReplicaSet();

        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
        ));

        $batch = $this->getBatch(array('w' => 99, 'wTimeoutMS' => 1));
        $batch->add(array('q' => array('x' => 1), 'limit' => 1));
        $batch->add(array('q' => array('x' => 2), 'limit' => 1));
        $result = $batch->execute();

        $this->assertSame(2, $result['nRemoved']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertHasWriteConcernError($result);
        $this->assertCollectionContains(array());
    }

    public function testWriteErrorWithOrderedBatch()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
        ));

        $batch = $this->getBatch(array('ordered' => true));
        $batch->add(array('q' => array('x' => 1), 'limit' => 1));
        $batch->add(array('q' => array('$where' => '3rr0r'), 'limit' => 1));
        $batch->add(array('q' => array('$where' => '3rr0r'), 'limit' => 1));
        $batch->add(array('q' => array('x' => 2), 'limit' => 1));
        $result = $batch->execute();

        $this->assertSame(1, $result['nRemoved']);
        $this->assertNumWriteErrors(1, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 2, 'x' => 2),
        ));
    }

    public function testWriteErrorWithUnorderedBatch()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
        ));

        $batch = $this->getBatch(array('ordered' => false));
        $batch->add(array('q' => array('x' => 1), 'limit' => 1));
        $batch->add(array('q' => array('$where' => '3rr0r'), 'limit' => 1));
        $batch->add(array('q' => array('$where' => '3rr0r'), 'limit' => 1));
        $batch->add(array('q' => array('x' => 2), 'limit' => 1));
        $result = $batch->execute();

        $this->assertSame(2, $result['nRemoved']);
        $this->assertNumWriteErrors(2, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array());
    }

    public function testSingleDeleteOperationWithLimit()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 1),
            array('_id' => 3, 'x' => 1),
        ));

        $batch = $this->getBatch();
        $batch->add(array('q' => array('x' => 1), 'limit' => 1));
        $result = $batch->execute();

        $this->assertSame(1, $result['nRemoved']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 2, 'x' => 1),
            array('_id' => 3, 'x' => 1),
        ));
    }

    public function testSingleDeleteOperationWithoutLimit()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 1),
            array('_id' => 3, 'x' => 1),
            array('_id' => 4, 'x' => 2),
        ));

        $batch = $this->getBatch();
        $batch->add(array('q' => array('x' => 1), 'limit' => 0));
        $result = $batch->execute();

        $this->assertSame(3, $result['nRemoved']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 4, 'x' => 2),
        ));
    }

    public function testMultipleDeleteOperations()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 1),
            array('_id' => 3, 'x' => 2),
            array('_id' => 4, 'x' => 2),
            array('_id' => 5, 'x' => 3),
        ));

        $batch = $this->getBatch();
        $batch->add(array('q' => array('x' => 1), 'limit' => 0));
        $batch->add(array('q' => array('x' => 2), 'limit' => 1));
        $result = $batch->execute();

        $this->assertSame(3, $result['nRemoved']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 4, 'x' => 2),
            array('_id' => 5, 'x' => 3),
        ));
    }

    public function testGetItemCount()
    {
        $batch = $this->getBatch();

        $operation = array(
            'q' => array('foo' => 1),
            'limit' => 1,
        );

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

        $this->assertSame(BatchInterface::OP_DELETE, $batch->getType());
    }
}
