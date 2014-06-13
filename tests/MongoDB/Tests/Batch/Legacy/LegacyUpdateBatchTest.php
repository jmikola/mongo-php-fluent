<?php

namespace MongoDB\Tests\Batch\Legacy;

use MongoDB\Batch\Legacy\LegacyUpdateBatch;
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
    public function testUpsertedIdExtractionFromGetLastErrorResponse()
    {
        $collection = $this->getMongoCollection();

        $batch = $this->getBatch();
        $batch->add(array(
            'q' => array('_id' => 1),
            'u' => array('$set' => array('x' => 1)),
            'upsert' => true,
        ));
        $result = $batch->execute();

        $this->assertSame(1, $result['nUpserted']);
        $this->assertSame(1, $result['upserted'][0]['_id']);
        $this->assertNotNull($collection->findOne(array('_id' => 1)));

        $batch = $this->getBatch();
        $batch->add(array(
            'q' => array('x' => 2),
            'u' => array('_id' => 2),
            'upsert' => true,
        ));
        $result = $batch->execute();

        $this->assertSame(1, $result['nUpserted']);
        $this->assertSame(2, $result['upserted'][0]['_id']);
        $this->assertNotNull($collection->findOne(array('_id' => 2)));

        $batch = $this->getBatch();
        $batch->add(array(
            'q' => array('_id' => 4),
            'u' => array('_id' => 3),
            'upsert' => true,
        ));
        $result = $batch->execute();

        $this->assertSame(1, $result['nUpserted']);
        $this->assertSame(3, $result['upserted'][0]['_id']);
        $this->assertNotNull($collection->findOne(array('_id' => 3)));
        $this->assertNull($collection->findOne(array('_id' => 4)));
    }

    /**
     * @see AbstractUpdateBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new LegacyUpdateBatch($this->getMongoDB(), $this->getMongoCollection(), $writeOptions);
    }
}
