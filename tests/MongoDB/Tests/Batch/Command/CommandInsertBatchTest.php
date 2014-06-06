<?php

namespace MongoDB\Tests\Batch\Command;

use MongoDB\Batch\Command\CommandInsertBatch;
use MongoDB\Tests\Batch\AbstractInsertBatchTest;

class CommandInsertBatchTest extends AbstractInsertBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresWriteCommands();
    }

    /**
     * @see AbstractInsertBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        $db = $this->getMongoDB();
        $collection = $this->getMongoCollection();

        return new CommandInsertBatch($collection, $writeOptions);
    }
}
