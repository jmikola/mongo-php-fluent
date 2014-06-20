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
     * @expectedException MongoWriteConcernException
     */
    public function testCommandFailsIfWriteConcernUsedWithStandalone()
    {
        $this->requiresStandalone();

        $batch = $this->getBatch(array('w' => 2));
        $batch->add(array('q' => array('_id' => 1), 'u' => array('$set' => array('x' => 1))));
        $result = $batch->execute();
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
