<?php

namespace MongoDB\Tests\Batch\Command;

use MongoDB\Batch\Command\CommandInsertBatch;
use MongoDB\Tests\Batch\AbstractBatchTest;
use MongoDB\Tests\Batch\AbstractInsertBatchTest;

class CommandInsertBatchTest extends AbstractInsertBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresWriteCommands();
    }

    /**
     * @see AbstractBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new CommandInsertBatch($this->getMongoCollection(), $writeOptions);
    }
}
