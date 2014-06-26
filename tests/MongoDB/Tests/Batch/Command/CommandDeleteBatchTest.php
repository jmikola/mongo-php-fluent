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
     * @expectedException MongoWriteConcernException
     */
    public function testCommandFailsIfJournaledWriteConcernUsedAndJournalingDisabled()
    {
        $this->requiresNoJournal();

        $batch = $this->getBatch(array('j' => true));
        $batch->add(array('q' => array(), 'limit' => 1));
        $result = $batch->execute();
    }

    /**
     * @expectedException MongoWriteConcernException
     */
    public function testCommandFailsIfWriteConcernUsedWithStandalone()
    {
        $this->requiresStandalone();

        $batch = $this->getBatch(array('w' => 2));
        $batch->add(array('q' => array(), 'limit' => 1));
        $result = $batch->execute();
    }

    /**
     * @see AbstractBatchTest::getBatch()
     */
    protected function getBatch(array $writeOptions = array())
    {
        return new CommandDeleteBatch($this->getMongoCollection(), $writeOptions);
    }
}
