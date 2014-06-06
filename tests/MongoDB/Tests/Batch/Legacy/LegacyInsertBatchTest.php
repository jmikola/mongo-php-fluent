<?php

namespace MongoDB\Tests\Batch\Legacy;

use MongoDB\Batch\Legacy\LegacyInsertBatch;
use MongoDB\Tests\Batch\AbstractInsertBatchTest;

class LegacyInsertBatchTest extends AbstractInsertBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresLegacyWriteOperations();
    }

    /**
     * @see AbstractInsertBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        $db = $this->getMongoDB();
        $collection = $this->getMongoCollection();

        return new LegacyInsertBatch($db, $collection, $writeOptions);
    }
}
