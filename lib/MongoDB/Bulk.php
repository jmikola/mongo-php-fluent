<?php

namespace MongoDB;

use MongoDB\Exception\UnexpectedTypeException;
use BadMethodCallException;
use InvalidArgumentException;
use MongoCollection;
use MongoId;
use RuntimeException;
use stdClass;

abstract class Bulk implements BulkInterface
{
    private $collection;
    private $currentOp;
    private $executed = false;
    private $useWriteCommands = true;

    /**
     * Constructor.
     *
     * @param MongoCollection $collection
     */
    final public function __construct(MongoCollection $collection)
    {
        $this->collection = $collection;
    }

    /**
     * Executes all scheduled write operations.
     *
     * @see BulkInterface::execute()
     * @param array $writeConcern
     * @return array
     * @throws BadMethodCallException if the bulk operations have already been executed
     */
    public function execute(array $writeConcern = null)
    {
        if ($this->executed) {
            throw new BadMethodCallException('Cannot call execute() multiple times');
        }

        $bulkResult = array(
            'nInserted' => 0,
            'nUpserted' => 0,
            'nUpdated' => 0,
            'nModified' => 0,
            'nRemoved' => 0,
            'writeErrors' => array(),
            'writeConcernErrors' => array(),
            'upserted' => array(),
        );

        $ordered = $this->isOrdered();

        foreach ($this->getBatches() as $batch) {
            if ($this->useWriteCommands) {
                $batchResult = $this->executeBatchWithWriteOps($batch, $writeConcern);
            } else {
                $batchResult = $this->executeBatchWithLegacyOps($batch, $writeConcern);
            }

            $this->mergeBatchResult($batch, $batchResult, $bulkResult);

            /* If we have errors and they are not all replication errors, abort
             * immediately. Ordered batches cannot enforce full-batch write
             * concerns if they fail, so clear "writeConcernErrors" as well.
             */
            if ($ordered && ! empty($bulkResult['writeErrors'])) {
                $bulkResult['writeConcernErrors'] = array();
                break;
            }
        }

        $this->executed = true;

        return $bulkResult;
    }

    /**
     * Sets the query selector for the next update or remove operation.
     *
     * @see BulkInterface::find()
     * @param array|object $query
     * @return self
     * @throws UnexpectedTypeException if $query is neither an array nor an object
     */
    public function find($query)
    {
        if (is_array($query)) {
            $query = (object) $query;
        }

        if ( ! is_object($query)) {
            throw new UnexpectedTypeException($query, 'array or object');
        }

        $this->currentOp['q'] = $query;

        return $this;
    }

    /**
     * Adds an insert operation for a document.
     *
     * @see BulkInterface::insert()
     * @param array|object $document
     * @throws UnexpectedTypeException if $document is neither an array nor an object
     */
    public function insert($document)
    {
        if (is_array($document)) {
            $document = (object) $document;
        }

        if ( ! is_object($document)) {
            throw new UnexpectedTypeException($document, 'array or object');
        }

        $this->addOperation(BulkInterface::OP_INSERT, $document);
    }

    /**
     * Adds a remove operation for all documents matching the current selector.
     *
     * @see BulkInterface::remove()
     */
    public function remove()
    {
        $document = array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'limit' => 0,
        );

        $this->currentOp = array();

        $this->addOperation(BulkInterface::OP_REMOVE, $document);
    }

    /**
     * Adds a remove operation for one document matching the current selector.
     *
     * @see BulkInterface::removeOne()
     */
    public function removeOne()
    {
        $document = array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'limit' => 1,
        );

        $this->currentOp = array();

        $this->addOperation(BulkInterface::OP_REMOVE, $document);
    }

    /**
     * Adds an update operation for all documents matching the current selector.
     *
     * If the upsert option is true, only a single document will be updated.
     *
     * @see BulkInterface::update()
     * @param array|object $newObj
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    public function update($newObj)
    {
        if (is_array($newObj)) {
            $newObj = (object) $newObj;
        }

        if ( ! is_object($newObj)) {
            throw new UnexpectedTypeException($newObj, 'array or object');
        }

        $document = array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'u' => $newObj,
            'multi' => empty($this->currentOp['upsert']),
            'upsert' => ( ! empty($this->currentOp['upsert'])),
        );

        $this->currentOp = array();

        $this->addOperation(BulkInterface::OP_UPDATE, $document);
    }

    /**
     * Adds an update operation for one document matching the current selector.
     *
     * @see BulkInterface::updateOne()
     * @param array|object $newObj
     * @throws UnexpectedTypeException if $newObj is neither an array nor an object
     */
    public function updateOne($newObj)
    {
        if (is_array($newObj)) {
            $newObj = (object) $newObj;
        }

        if ( ! is_object($newObj)) {
            throw new UnexpectedTypeException($newObj, 'array or object');
        }

        $document = array(
            'q' => empty($this->currentOp['q']) ? new stdClass : $this->currentOp['q'],
            'u' => $newObj,
            'multi' => false,
            'upsert' => ( ! empty($this->currentOp['upsert'])),
        );

        $this->currentOp = array();

        $this->addOperation(BulkInterface::OP_UPDATE, $document);
    }

    /**
     * Set the upsert option for the next update operation.
     *
     * @see BulkInterface::upsert()
     * @param boolean $upsert
     * @return self
     */
    public function upsert($upsert = true)
    {
        $this->currentOp['upsert'] = (boolean) $upsert;

        return $this;
    }

    /**
     * Adds a write operation.
     *
     * @param integer      $type
     * @param array|object $document
     * @throws InvalidArgumentException if the document BSON size exceeds
     *                                  BulkInterface::MAX_BATCH_SIZE_BYTES
     */
    abstract protected function addOperation($type, $document);

    /**
     * Get the batches to be executed.
     *
     * @return array
     */
    abstract protected function getBatches();

    /**
     * Return whether bulk operations are ordered.
     *
     * @return boolean
     */
    abstract protected function isOrdered();

    /**
     * Execute a batch using legacy write operations.
     *
     * @param Batch $batch
     * @param array $writeConcern
     * @return array
     */
    protected function executeBatchWithLegacyOps(Batch $batch, array $writeConcern = null)
    {
        $result = array(
            'n' => 0,
            'writeErrors' => array(),
        );

        $ordered = $this->isOrdered();

        foreach ($batch->getDocuments() as $batchIndex => $document) {
            if ($ordered && ! empty($result['writeErrors'])) {
                break;
            }

            $options = array('w' => 1);

            if ($this->type === BulkInterface::OP_INSERT) {
                $gle = $this->collection->insert($document, $options);
            }

            elseif ($this->type === BulkInterface::OP_UPDATE) {
                $options['upsert'] = $document['upsert'];
                $options['multiple'] = $document['multi'];
                $gle = $this->collection->update($document['q'], $document['u'], $options);
            }

            elseif ($this->type === BulkInterface::OP_REMOVE) {
                $options['justOne'] = ($document['limit'] === 1);
                $gle = $this->collection->remove($document['q'], $options);
            }

            $err = Util::extractGLEErrors($gle);

            if ($err['writeError'] !== null) {
                $result['writeErrors'][] = array(
                    'index' => $batchIndex,
                    'code' => $err['writeError']['code'],
                    'errmsg' => $err['writeError']['errmsg'],
                    'op' => $document,
                );
            }

            /* If a write error occurred, we know the insert was unsuccessful;
             * however, updates and removes might still have affected documents.
             */
            elseif ($this->type === BulkInterface::OP_INSERT) {
                $result['n'] += 1;
            }

            if ($this->type === BulkInterface::OP_UPDATE) {
                if ($document['upsert'] && empty($gle['updatedExisting'])) {
                    $result['n'] += 1;
                    $result['upserted'][] = array(
                        'index' => $batchIndex,
                        '_id' => $this->getUpsertedId($document, $gle),
                    );
                }

                elseif ( ! empty($gle['n'])) {
                    $result['n'] += (integer) $gle['n'];
                }
            }

            if ($this->type === BulkInterface::OP_REMOVE && ! empty($gle['n'])) {
                $result['n'] += (integer) $gle['n'];
            }
        }

        /* The write concern may have not been enforced if we did it earlier and
         * a write error occurs, so apply the actual write concern at the end.
         */
        if (empty($result['writeErrors']) || ! $ordered && (count($result['writeErrors']) < $batch->getSize())) {
            $this->collection->db->command(array('resetError' => 1));
            $gle = $this->collection->db->command(array('getlasterror' => 1) + ($writeConcern ?: array()));
            $err = Util::extractGLEErrors($gle);
        }

        if (isset($err) && $err['wcError'] !== null) {
            $bulkResult['writeConcernErrors'][] = $err['wcError'];
        }

        return $result;
    }

    /**
     * Execute a batch using write commands.
     *
     * @param Batch $batch
     * @param array $writeConcern
     * @return array
     * @throws RuntimeException if the write command fails
     */
    protected function executeBatchWithWriteOps(Batch $batch, array $writeConcern = null)
    {
        $type = $batch->getType();

        if ($type === BulkInterface::OP_INSERT) {
            $cmd = array(
                'insert' => $this->collection->getName(),
                'documents' => $this->addIdsIfNeeded($batch->getDocuments()),
                'ordered' => $this->isOrdered(),
            );
        }

        elseif ($type === BulkInterface::OP_UPDATE) {
            $cmd = array(
                'update' => $this->collection->getName(),
                'updates' => $batch->getDocuments(),
                'ordered' => $this->isOrdered(),
            );
        }

        elseif ($type === BulkInterface::OP_REMOVE) {
            $cmd = array(
                'delete' => $this->collection->getName(),
                'deletes' => $batch->getDocuments(),
                'ordered' => $this->isOrdered(),
            );
        }

        if ( ! empty($writeConcern)) {
            $cmd['writeConcern'] = $writeConcern;
        }

        $result = $this->collection->db->command($cmd);

        if (empty($result['ok'])) {
            throw new RuntimeException('Batch failed, cannot aggregate results: ' . $result['errmsg']);
        }

        return $result;
    }

    /**
     * Adds an ObjectId to each document without an _id field.
     *
     * @param array $documents
     * @return array
     */
    private function addIdsIfNeeded(array $documents)
    {
        foreach ($documents as $document) {
            if ( ! isset($document->_id) && ! array_key_exists('_id', get_object_vars($document))) {
                $document->_id = new MongoId;
            }
        }

        return $documents;
    }

    /**
     * Get the identifier for an upsert operation.
     *
     * Checking for "upserted" in the GLE response is not sufficient for MongoDB
     * 2.4, since the field will not exist unless the identifier was created
     * server-side by the upsert. Therefore, we should also check the update
     * operation's query and newObj documents.
     *
     * @see Bulk::executeBatchWithLegacyOps()
     * @see https://jira.mongodb.org/browse/DOCS-2589
     * @param array $document Update operation
     * @param array $gle      GLE response
     * @return mixed
     * @throws RuntimeException if the upserted identifier cannot be determined
     */
    private function getUpsertedId(array $operation, array $gle)
    {
        if (isset($gle['upserted']) || array_key_exists('upserted', $gle)) {
            return $gle['upserted'];
        }

        if (isset($operation['q']->_id) || array_key_exists('_id', get_object_vars($operation['q']))) {
            return $operation['q']->_id;
        }

        if (isset($operation['u']->_id) || array_key_exists('_id', get_object_vars($operation['u']))) {
            return $operation['u']->_id;
        }

        throw new RuntimeException('Could not determine upserted identifier');
    }

    /**
     * Merges a batch result into the bulk operation result.
     *
     * @param Batch $batch
     * @param array $batchResult
     * @param array $bulkResult
     */
    private function mergeBatchResult(Batch $batch, array $batchResult, array &$bulkResult)
    {
        /* TODO: If {w:0} was used, the batch result may omit everything except
         * the "ok" field. Ensure that at least "n" is present.
         */

        if ($batch->getType() === BulkInterface::OP_INSERT) {
            $bulkResult['nInserted'] += $batchResult['n'];
        }

        if ($batch->getType() === BulkInterface::OP_REMOVE) {
            $bulkResult['nRemoved'] += $batchResult['n'];
        }

        if ($batch->getType() === BulkInterface::OP_UPDATE) {
            $nModified = ! empty($batchResult['nModified']) ? $batchResult['nModified'] : 0;
            $nUpserted = 0;

            // Handle case where only a single document was updated
            if (isset($batchResult['upserted']['index'])) {
                $batchResult['upserted'] = array($batchResult['upserted']);
            }

            if ( ! empty($batchResult['upserted']) && is_array($batchResult['upserted'])) {
                foreach ($batchResult['upserted'] as $upsert) {
                    $upsert['index'] = $batch->getBulkIndex($upsert['index']);
                    $bulkResult['upserted'][] = $upsert;
                }

                $nUpserted = count($batchResult['upserted']);
            }

            $bulkResult['nModified'] += $nModified;
            $bulkResult['nUpserted'] += $nUpserted;
            $bulkResult['nUpdated'] += $batchResult['n'] - $nUpserted;
        }

        if ( ! empty($batchResult['writeErrors']) && is_array($batchResult['writeErrors'])) {
            foreach ($batchResult['writeErrors'] as $writeError) {
                $writeError['op'] = $batch->getDocument($writeError['index']);
                $writeError['index'] = $batch->getBulkIndex($writeError['index']);
                $bulkResult['writeErrors'][] = $writeError;
            }
        }

        // TODO: writeConcernErrors ultimately need to be aggregated

        if ( ! empty($batchResult['writeConcernError'])) {
            $bulkResult['writeConcernErrors'][] = $batchResult['writeConcernError'];
        }
    }
}
