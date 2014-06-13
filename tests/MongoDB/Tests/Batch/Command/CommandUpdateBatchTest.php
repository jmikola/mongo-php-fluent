<?php

namespace MongoDB\Tests\Batch\Command;

use MongoDB\Batch\Command\CommandUpdateBatch;
use MongoDB\Tests\Batch\AbstractUpdateBatchTest;

class CommandUpdateBatchTest extends AbstractUpdateBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresWriteCommands();
    }

    /**
     * @see AbstractUpdateBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new CommandUpdateBatch($this->getMongoCollection(), $writeOptions);
    }
}
