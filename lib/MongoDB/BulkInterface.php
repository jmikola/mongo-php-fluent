<?php

namespace MongoDB;

use MongoDB\Exception\UnexpectedTypeException;
use InvalidArgumentException;

interface BulkInterface
{
    const OP_INSERT = 1;
    const OP_UPDATE = 2;
    const OP_REMOVE = 3;

    const MAX_BATCH_SIZE_DOCS = 1000;
    const MAX_BATCH_SIZE_BYTES = 16777216;

    /**
     * Sets the query selector for the next update or remove operation.
     *
     * @param array|object $query
     * @return self
     * @throws UnexpectedTypeException if $query is neither an array nor an object
     */
    public function find($query);

    /**
     * Adds an operation to insert a document.
     *
     * @param array|object $document
     * @throws UnexpectedTypeException if $document is neither an array nor an object
     */
    public function insert($document);

    /**
     * Adds a remove operation for all documents matching the current selector.
     */
    public function remove();

    /**
     * Adds a remove operation for one document matching the current selector.
     */
    public function removeOne();

    /**
     * Adds an update operation for all documents matching the current selector.
     *
     * If the upsert option is true, only a single document will be updated.
     *
     * @param array|object $newObj
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    public function update($newObj);

    /**
     * Adds an update operation for one document matching the current selector.
     *
     * @param array|object $newObj
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    public function updateOne($newObj);

    /**
     * Set the upsert option for the next update operation.
     *
     * @param boolean $upsert
     * @return self
     */
    public function upsert($upsert = true);
}
