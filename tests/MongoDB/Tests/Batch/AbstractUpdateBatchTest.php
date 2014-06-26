<?php

namespace MongoDB\Tests\Batch;

use MongoDB\Batch\BatchInterface;

abstract class AbstractUpdateBatchTest extends AbstractBatchFunctionalTest
{
    public function testWriteConcernTimeoutError()
    {
        $this->requiresReplicaSet();

        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
            array('_id' => 3, 'x' => 3),
        ));

        $batch = $this->getBatch(array('w' => 99, 'wTimeoutMS' => 1));
        $batch->add(array('q' => array('_id' => 1), 'u' => array('x' => 3)));
        $batch->add(array('q' => array('_id' => 2), 'u' => array('x' => 3)));
        $result = $batch->execute();

        $this->assertSame(2, $result['nMatched']);
        $this->assertNumModified(2, $result);
        $this->assertSame(0, $result['nUpserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertHasWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 3),
            array('_id' => 2, 'x' => 3),
            array('_id' => 3, 'x' => 3),
        ));
    }

    public function testWriteErrorWithOrderedBatch()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
            array('_id' => 3, 'x' => 3),
        ));

        $batch = $this->getBatch(array('ordered' => true));
        $batch->add(array('q' => array('_id' => 1), 'u' => array('x' => 3)));
        $batch->add(array('q' => array('$where' => '3rr0r'), 'u' => array('x' => 4)));
        $batch->add(array('q' => array('_id' => 2), 'u' => array('x' => 3)));
        $result = $batch->execute();

        $this->assertSame(1, $result['nMatched']);
        $this->assertNumModified(1, $result);
        $this->assertSame(0, $result['nUpserted']);
        $this->assertNumWriteErrors(1, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 3),
            array('_id' => 2, 'x' => 2),
            array('_id' => 3, 'x' => 3),
        ));
    }

    public function testWriteErrorWithUnorderedBatch()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 2),
            array('_id' => 3, 'x' => 3),
        ));

        $batch = $this->getBatch(array('ordered' => false));
        $batch->add(array('q' => array('_id' => 1), 'u' => array('x' => 3)));
        $batch->add(array('q' => array('$where' => '3rr0r'), 'u' => array('x' => 4)));
        $batch->add(array('q' => array('_id' => 2), 'u' => array('x' => 3)));
        $result = $batch->execute();

        $this->assertSame(2, $result['nMatched']);
        $this->assertNumModified(2, $result);
        $this->assertSame(0, $result['nUpserted']);
        $this->assertNumWriteErrors(1, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 3),
            array('_id' => 2, 'x' => 3),
            array('_id' => 3, 'x' => 3),
        ));
    }

    public function testSingleUpdateOperation()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 3),
            array('_id' => 2, 'x' => 3),
            array('_id' => 3, 'x' => 3),
        ));

        $batch = $this->getBatch();
        $batch->add(array('q' => array('x' => 3), 'u' => array('$set' => array('x' => 1))));
        $result = $batch->execute();

        $this->assertSame(1, $result['nMatched']);
        $this->assertNumModified(1, $result);
        $this->assertSame(0, $result['nUpserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
            array('_id' => 2, 'x' => 3),
            array('_id' => 3, 'x' => 3),
        ));
    }

    public function testSingleUpdateOperationWithMulti()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1, 'x' => 3, 'y' => 0),
            array('_id' => 2, 'x' => 3, 'y' => 0),
            array('_id' => 3, 'x' => 3, 'y' => 1),
        ));

        $batch = $this->getBatch();
        $batch->add(array('q' => array('x' => 3), 'u' => array('$set' => array('y' => 1)), 'multi' => true));
        $result = $batch->execute();

        $this->assertSame(3, $result['nMatched']);
        $this->assertNumModified(2, $result);
        $this->assertSame(0, $result['nUpserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 3, 'y' => 1),
            array('_id' => 2, 'x' => 3, 'y' => 1),
            array('_id' => 3, 'x' => 3, 'y' => 1),
        ));
    }

    public function testSingleUpdateOperationWithUpsertAndExistingDocument()
    {
        $this->loadIntoCollection(array(
            array('_id' => 1),
        ));

        $batch = $this->getBatch();
        $batch->add(array('q' => array('_id' => 1), 'u' => array('$set' => array('x' => 1)), 'upsert' => true));
        $result = $batch->execute();

        $this->assertSame(1, $result['nMatched']);
        $this->assertNumModified(1, $result);
        $this->assertSame(0, $result['nUpserted']);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
        ));
    }

    public function testSingleUpdateOperationWithUpsertAndNoExistingDocument()
    {
        $batch = $this->getBatch();
        $batch->add(array('q' => array('_id' => 1), 'u' => array('$set' => array('x' => 1)), 'upsert' => true));
        $result = $batch->execute();

        $this->assertSame(0, $result['nMatched']);
        $this->assertNumModified(0, $result);
        $this->assertSame(1, $result['nUpserted']);
        $this->assertUpsertedIdForIndex(1, 0, $result);
        $this->assertNumWriteErrors(0, $result);
        $this->assertDoesNotHaveWriteConcernError($result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
        ));
    }

    public function testGetItemCount()
    {
        $batch = $this->getBatch();

        $operation = array(
            'q' => array('foo' => 1),
            'u' => array('foo' => array('$inc' => 1)),
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

        $this->assertSame(BatchInterface::OP_UPDATE, $batch->getType());
    }

    protected function assertUpsertedIdForIndex($id, $index, $result)
    {
        $this->assertArrayHasKey('upserted', $result);
        $this->assertInternalType('array', $result['upserted']);
        $this->assertContains(array('index' => $index, '_id' => $id), $result['upserted']);
    }

    abstract protected function assertNumModified($nModified, $result);
}
