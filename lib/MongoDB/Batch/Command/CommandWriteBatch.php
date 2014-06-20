<?php

namespace MongoDB\Batch\Command;

use MongoDB\Batch\BatchInterface;
use MongoWriteBatch;
use MongoWriteConcernException;

abstract class CommandWriteBatch implements BatchInterface
{
    /**
     * Constructor.
     *
     * @param MongoWriteBatch $batch
     */
    protected function __construct(MongoWriteBatch $batch)
    {
        $this->batch = $batch;
    }

    /**
     * @see BatchInterface::getItemCount()
     */
    public function add($document)
    {
        $this->batch->add($document);
    }

    /**
     * @see BatchInterface::execute()
     */
    public function execute(array $writeOptions = array())
    {
        try {
            $result = $this->batch->execute($writeOptions);
        } catch (MongoWriteConcernException $e) {
            $result = $e->getDocument();

            if (empty($result['ok'])) {
                throw $e;
            }
        }

        return $result;
    }

    /**
     * @see BatchInterface::getItemCount()
     */
    public function getItemCount()
    {
        return $this->batch->getItemCount();
    }
}
