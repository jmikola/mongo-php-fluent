<?php

namespace MongoDB\Batch;

use MongoDB\Exception\UnexpectedTypeException;
use MongoException;

interface BatchInterface
{
    const OP_INSERT = 1;
    const OP_UPDATE = 2;
    const OP_DELETE = 3;

    /**
     * Adds an operation to the batch.
     *
     * @param array|object $document
     */
    public function add($document);

    /**
     * Execute the batch of write operations.
     *
     * @param array $writeOptions Write concern and ordered options.
     * @return array
     * @throws MongoException if the batch is empty
     */
    public function execute(array $writeOptions = array());

    /**
     * Return the number of operations in this batch.
     *
     * @return integer
     */
    public function getItemCount();

    /**
     * Return the type of operations in this batch.
     *
     * @return integer
     */
    public function getType();
}
