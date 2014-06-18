<?php

namespace MongoDB\Tests\Batch\Command;

use MongoDB\Batch\Command\CommandUpdateBatch;
use MongoDB\Tests\Batch\AbstractBatchTest;
use MongoDB\Tests\Batch\AbstractUpdateBatchTest;

class CommandUpdateBatchTest extends AbstractUpdateBatchTest
{
    public function setUp()
    {
        parent::setUp();
        $this->requiresWriteCommands();
    }

    /**
     * @see AbstractUpdateBatchTest::assertNumModified()
     */
    protected function assertNumModified($nModified, $result)
    {
        $this->assertSame($nModified, $result['nModified']);
    }

    /**
     * @see AbstractBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new CommandUpdateBatch($this->getMongoCollection(), $writeOptions);
    }
}
