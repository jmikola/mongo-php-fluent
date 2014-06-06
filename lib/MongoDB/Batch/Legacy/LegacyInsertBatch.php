<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoDB\Exception\UnexpectedTypeException;
use MongoException;

final class LegacyInsertBatch extends LegacyWriteBatch
{
    /**
     * @see BatchInterface::add()
     * @throws UnexpectedTypeException if $document is neither an array nor an object
     */
    public function add($document)
    {
        if ( ! is_array($document) && ! is_object($document)) {
            throw new UnexpectedTypeException($document, 'array or object');
        }

        $this->documents[] = $document;
    }

    /**
     * @see BatchInterface::execute()
     */
    public function execute(array $writeOptions = array())
    {
        if (empty($this->documents)) {
            throw new MongoException('Cannot call execute() for an empty batch');
        }

        $writeOptions = array_merge($this->writeOptions, $writeOptions);

        $result = array(
            'nInserted' => 0,
            'writeErrors' => array(),
            'writeConcernErrors' => array(),
        );

        foreach ($this->documents as $batchIndex => $document) {
            if ($writeOptions['ordered'] && ! empty($result['writeErrors'])) {
                break;
            }

            // TODO: Catch exceptions and capture GLE responses
            $gle = $this->collection->insert($document, array('w' => 1));

            $err = $this->parseGetLastErrorResponse($gle);

            if ($err['writeError'] !== null) {
                $result['writeErrors'][] = array(
                    'index' => $batchIndex,
                    'code' => $err['writeError']['code'],
                    'errmsg' => $err['writeError']['errmsg'],
                    'op' => $document,
                );

                continue;
            }

            $result['nInserted'] += 1;
        }

        if (empty($result['writeErrors']) ||
            ( ! $writeOptions['ordered'] && count($result['writeErrors']) < count($this->documents))) {

            $err = $this->applyWriteConcern($writeOptions);
        }

        if (isset($err) && $err['wcError'] !== null) {
            $result['writeConcernErrors'][] = $err['wcError'];
        }

        return $result;
    }

    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_INSERT;
    }
}
