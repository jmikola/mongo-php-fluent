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
        return new LegacyUpdateBatch($this->getMongoDB(), $this->getMongoCollection(), $writeOptions);
    }
}
