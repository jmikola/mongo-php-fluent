<?php

namespace MongoDB\Tests\Batch\Legacy;

use MongoDB\Batch\Legacy\LegacyUpdateBatch;
use MongoDB\Tests\Batch\AbstractBatchTest;
use MongoDB\Tests\Batch\AbstractUpdateBatchTest;

class LegacyUpdateBatchTest extends AbstractUpdateBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresLegacyWriteOperations();
    }

    /**
     * @see https://jira.mongodb.org/browse/PHP-1116
     */
    public function testUpsertedIdShouldBeExtractedFromQueryIfNotInGetLastErrorResponse()
    {
        $batch = $this->getBatch();
        $batch->add(array(
            'q' => array('_id' => 1),
            'u' => array('$set' => array('x' => 1)),
            'upsert' => true,
        ));
        $this->executeBatch($batch);

        $this->assertSame(1, $result['nUpserted']);
        $this->assertUpsertedIdForIndex(1, 0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 1, 'x' => 1),
        ));
    }

    /**
     * @see https://jira.mongodb.org/browse/PHP-1116
     */
    public function testUpsertedIdShouldBeExtractedFromNewObjIfNotInGetLastErrorResponse()
    {
        $batch = $this->getBatch();
        $batch->add(array(
            'q' => array('x' => 2),
            'u' => array('_id' => 2),
            'upsert' => true,
        ));
        $this->executeBatch($batch);

        $this->assertSame(1, $result['nUpserted']);
        $this->assertUpsertedIdForIndex(2, 0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 2),
        ));
    }

    /**
     * @see https://jira.mongodb.org/browse/PHP-1116
     */
    public function testUpsertedIdShouldBeExtractedFromNewObjBeforeQueryIfNotInGetLastErrorResponse()
    {
        $batch = $this->getBatch();
        $batch->add(array(
            'q' => array('_id' => 4),
            'u' => array('_id' => 3),
            'upsert' => true,
        ));
        $this->executeBatch($batch);

        $this->assertSame(1, $result['nUpserted']);
        $this->assertUpsertedIdForIndex(3, 0, $result);
        $this->assertCollectionContains(array(
            array('_id' => 3),
        ));
    }

    /**
     * @see AbstractUpdateBatchTest::assertNumModified()
     */
    protected function assertNumModified($nModified, $result)
    {
        /* We cannot reliably calculate nModified for legacy batch updates, so
         * the result field must be set to null.
         */
        $this->assertNull($result['nModified']);
    }

    /**
     * @see AbstractBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new LegacyUpdateBatch($this->getMongoDB(), $this->getMongoCollection(), $writeOptions);
    }
}
