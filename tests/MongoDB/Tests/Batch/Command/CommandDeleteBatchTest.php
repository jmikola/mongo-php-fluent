<?php

namespace MongoDB\Tests\Batch\Command;

use MongoDB\Batch\Command\CommandDeleteBatch;
use MongoDB\Tests\Batch\AbstractBatchTest;
use MongoDB\Tests\Batch\AbstractDeleteBatchTest;

class CommandDeleteBatchTest extends AbstractDeleteBatchTest
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
        return new CommandDeleteBatch($this->getMongoCollection(), $writeOptions);
    }
}
