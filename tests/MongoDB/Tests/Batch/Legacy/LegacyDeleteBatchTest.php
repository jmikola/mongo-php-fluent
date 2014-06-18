<?php

namespace MongoDB\Tests\Batch\Legacy;

use MongoDB\Batch\Legacy\LegacyDeleteBatch;
use MongoDB\Tests\Batch\AbstractBatchTest;
use MongoDB\Tests\Batch\AbstractDeleteBatchTest;

class LegacyDeleteBatchTest extends AbstractDeleteBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresLegacyWriteOperations();
    }

    /**
     * @expectedException MongoException
     * @expectedExceptionMessage Excepted 'limit' to be 0 or 1; given: 2
     */
    public function testAddShouldThrowExceptionForInvalidLimit()
    {
        $batch = $this->getBatch();
        $batch->add(array('q' => array('_id' => 1), 'limit' => 2));
    }

    /**
     * @see AbstractBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new LegacyDeleteBatch($this->getMongoDB(), $this->getMongoCollection(), $writeOptions);
    }
}
