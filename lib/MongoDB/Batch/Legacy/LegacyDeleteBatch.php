<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoException;

final class LegacyDeleteBatch extends LegacyWriteBatch
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
            'nRemoved' => 0,
            'writeErrors' => array(),
            'writeConcernErrors' => array(),
        );

        foreach ($this->documents as $batchIndex => $document) {
            if ($writeOptions['ordered'] && ! empty($result['writeErrors'])) {
                break;
            }

            // TODO: Catch exceptions and capture GLE responses
            $gle = $this->collection->remove($document['q'], array(
                'w' => 1,
                'justOne' => ($document['limit'] === 1),
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

            if ( ! empty($gle['n'])) {
                $result['nRemoved'] += (integer) $gle['n'];
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
        return BatchInterface::OP_DELETE;
    }
}
