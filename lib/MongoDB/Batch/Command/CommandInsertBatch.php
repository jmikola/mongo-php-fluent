<?php

namespace MongoDB\Batch\Command;

use MongoDB\Batch\BatchInterface;
use MongoInsertBatch;

final class CommandInsertBatch extends MongoInsertBatch implements BatchInterface
{
    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_INSERT;
    }
}
