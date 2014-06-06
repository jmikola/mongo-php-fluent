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
     * @see AbstractUpdateBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        $db = $this->getMongoDB();
        $collection = $this->getMongoCollection();

        return new LegacyUpdateBatch($db, $collection, $writeOptions);
    }
}
