<?php

namespace MongoDB\Batch\Command;

use MongoDB\Batch\BatchInterface;
use MongoDeleteBatch;

final class CommandDeleteBatch extends MongoDeleteBatch implements BatchInterface
{
    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_DELETE;
    }
}
