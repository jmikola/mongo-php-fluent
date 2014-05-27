<?php

namespace MongoDB\Batch\Command;

use MongoDB\Batch\BatchInterface;
use MongoUpdateBatch;

final class CommandUpdateBatch extends MongoUpdateBatch implements BatchInterface
{
    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_UPDATE;
    }
}
