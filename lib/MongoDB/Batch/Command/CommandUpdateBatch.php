<?php

namespace MongoDB\Batch\Command;

use MongoDB\Batch\BatchInterface;
use MongoCollection;
use MongoUpdateBatch;

final class CommandUpdateBatch extends CommandWriteBatch
{
    /**
     * Constructor.
     *
     * @param MongoCollection $collection
     * @param array           $writeOptions Write concern and ordered options.
     *                                      Ordered will default to true.
     */
    public function __construct(MongoCollection $collection, array $writeOptions = array())
    {
        parent::__construct(new MongoUpdateBatch($collection, $writeOptions));
    }

    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_UPDATE;
    }
}
