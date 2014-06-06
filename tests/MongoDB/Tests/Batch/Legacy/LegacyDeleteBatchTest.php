<?php

namespace MongoDB\Tests\Batch\Legacy;

use MongoDB\Batch\Legacy\LegacyDeleteBatch;
use MongoDB\Tests\Batch\AbstractDeleteBatchTest;

class LegacyDeleteBatchTest extends AbstractDeleteBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresLegacyWriteOperations();
    }

    /**
     * @see AbstractDeleteBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        $db = $this->getMongoDB();
        $collection = $this->getMongoCollection();

        return new LegacyDeleteBatch($db, $collection, $writeOptions);
    }
}
