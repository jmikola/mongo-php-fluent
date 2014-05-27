<?php

namespace MongoDB\Bulk;

use MongoDB\Exception\UnexpectedTypeException;
use BadMethodCallException;

interface BulkInterface
{
    /**
     * Executes all scheduled write operations.
     *
     * @param array $writeConcern
     * @return array
     * @throws BadMethodCallException if the bulk operations have already been executed
     */
    public function execute(array $writeConcern = array());

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
     * Return whether bulk operations are ordered.
     *
     * @return boolean
     */
    public function isOrdered();

    /**
     * Adds a remove operation for all documents matching the current selector.
     *
     * @throws BadMethodCallException if find() has not been called previously
     */
    public function remove();

    /**
     * Adds a remove operation for one document matching the current selector.
     *
     * @throws BadMethodCallException if find() has not been called previously
     */
    public function removeOne();

    /**
     * Adds an update operation for all documents matching the current selector.
     *
     * If the upsert option is true, only a single document will be updated.
     *
     * @param array|object $newObj
     * @throws BadMethodCallException if find() has not been called previously
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    public function update($newObj);

    /**
     * Adds an update operation for one document matching the current selector.
     *
     * @param array|object $newObj
     * @throws BadMethodCallException if find() has not been called previously
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
