<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoException;
use RuntimeException;

final class LegacyUpdateBatch extends LegacyWriteBatch
{
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
            'nMatched' => 0,
            'nModified' => null,
            'nUpserted' => 0,
            'writeErrors' => array(),
            'writeConcernErrors' => array(),
        );

        foreach ($this->documents as $batchIndex => $document) {
            if ($writeOptions['ordered'] && ! empty($result['writeErrors'])) {
                break;
            }

            // TODO: Catch exceptions and capture GLE responses
            $gle = $this->collection->update($document['q'], $document['u'], array(
                'w' => 1,
                'upsert' => $document['upsert'],
                'multiple' => $document['multi'],
            ));

            $err = $this->parseGetLastErrorResponse($gle);

            if ($err['writeError'] !== null) {
                $result['writeErrors'][] = array(
                    'index' => $batchIndex,
                    'code' => $err['writeError']['code'],
                    'errmsg' => $err['writeError']['errmsg'],
                    'op' => $document,
                );
            }

            if ($document['upsert'] && empty($gle['updatedExisting'])) {
                $result['nUpserted'] += 1;
                $result['upserted'][] = array(
                    'index' => $batchIndex,
                    '_id' => $this->getUpsertedId($document, $gle),
                );

                continue;
            }

            if ( ! empty($gle['n'])) {
                $result['nMatched'] += (integer) $gle['n'] - (empty($gle['updatedExisting']) ? 0 : 1);
            }
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
        return BatchInterface::OP_UPDATE;
    }

    /**
     * Get the identifier for an upsert operation.
     *
     * Checking for "upserted" in the getLastError response is not sufficient
     * for MongoDB 2.4, since the field will not exist unless the identifier was
     * created server-side by the upsert. Therefore, we should also check the
     * update operation's query and newObj documents.
     *
     * @see LegacyUpdateBatch::execute()
     * @see https://jira.mongodb.org/browse/DOCS-2589
     * @param array $document Update operation
     * @param array $gle      getLastError response
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
}
