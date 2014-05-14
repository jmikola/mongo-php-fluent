<?php

namespace MongoDB\Batch;

use MongoDB\Exception\UnexpectedTypeException;
use BadMethodCallException;

interface BatchInterface
{
    const OP_INSERT = 1;
    const OP_UPDATE = 2;
    const OP_DELETE = 3;

    /**
     * Adds an operation document to the batch.
     *
     * @param object  $document
     * @throws UnexpectedTypeException if $document is not an object
     */
    public function add($document);

    /**
     * Execute the batch of write operations.
     *
     * @param array $writeOptions Write concern and ordered options.
     * @return array
     * @throws BadMethodCallException if the batch is empty
     */
    public function execute(array $writeOptions = array());
}
