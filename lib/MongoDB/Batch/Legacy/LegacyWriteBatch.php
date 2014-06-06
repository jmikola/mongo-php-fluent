<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoCollection;
use MongoDB;
use MongoException;
use UnexpectedValueException;

abstract class LegacyWriteBatch implements BatchInterface
{
    const ERR_UNKNOWN_ERROR = 8;
    const ERR_WRITE_CONCERN_FAILED = 64;
    const ERR_UNKNOWN_REPL_WRITE_CONCERN = 79;
    const ERR_NOT_MASTER = 10107;
    const ERR_GLE_WMODE_CHANGED_INVALID = 14330;
    const ERR_NO_LONGER_PRIMARY = 19900;
    const ERR_NO_LONGER_PRIMARY_REPLICATEDTONUM = 16805;

    protected $collection;
    protected $db;
    protected $documents = array();
    protected $writeOptions = array('ordered' => true);

    /**
     * Constructor.
     *
     * @param MongoDB         $db
     * @param MongoCollection $collection
     * @param array           $writeOptions Write concern and ordered options.
     *                                      Ordered will default to true.
     */
    public function __construct(MongoDB $db, MongoCollection $collection, array $writeOptions = array())
    {
        $this->db = $db;
        $this->collection = $collection;
        $this->writeOptions = array_merge($this->writeOptions, $writeOptions);
    }

    /**
     * @see BatchInterface::execute()
     */
    final public function execute(array $writeOptions = array())
    {
        if (empty($this->documents)) {
            throw new MongoException('Cannot call execute() for an empty batch');
        }

        $result = $this->createEmptyResult();
        $writeOptions = array_merge($this->writeOptions, $writeOptions);

        foreach ($this->documents as $batchIndex => $document) {
            if ($writeOptions['ordered'] && ! empty($result['writeErrors'])) {
                break;
            }

            $this->executeSingleOperation($batchIndex, $document, $result);
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
     * @see BatchInterface::getItemCount()
     */
    final public function getItemCount()
    {
        return count($this->documents);
    }

    /**
     * Applies a write concern and returns the equivalent write command response
     * document.
     *
     * @param array $writeOptions
     * @return array
     */
    final protected function applyWriteConcern(array $writeOptions)
    {
        unset($writeOptions['ordered']);

        $this->db->command(array('resetError' => 1));
        $gle = $this->db->command(array('getlasterror' => 1) + $writeOptions);

        return $this->parseGetLastErrorResponse($gle);
    }

    /**
     * Parses a getLastError response and returns an equivalent write command
     * response document.
     *
     * This should kept be up to date with BatchSafeWriter::extractGLEErrors in
     * the MongoDB server.
     *
     * @param array $gle
     * @return array
     * @throws UnexpectedValueException if an unknown error is encountered
     */
    final protected function parseGetLastErrorResponse(array $gle)
    {
        $isOk    = ! empty($gle['ok']);
        $code    = ! empty($gle['code']) ? (integer) $gle['code'] : 0;
        $err     = ! empty($gle['err']) ? (string) $gle['err'] : '';
        $errMsg  = ! empty($gle['errmsg']) ? (string) $gle['errmsg'] : '';
        $jNote   = ! empty($gle['jnote']) ? (string) $gle['jnote'] : '';
        $wNote   = ! empty($gle['wnote']) ? (string) $gle['wnote'] : '';
        $timeout = ! empty($gle['wtimeout']);

        $extractedError = array(
            'writeError' => null,
            'wcError' => null
        );

        if ($err === 'norepl' || $err === 'noreplset') {
            // Know this is legacy gle and the repl not enforced - write concern error in 2.4
            $extractedError['wcError'] = array(
                'code' => self::ERR_WRITE_CONCERN_FAILED,
                'errmsg' => $errMsg ?: $wNote ?: $err,
            );

            return $extractedError;
        }

        if ($timeout) {
            // Know there was not write error
            $extractedError['wcError'] = array(
                'code' => self::ERR_WRITE_CONCERN_FAILED,
                'errmsg' => $errMsg ?: $err,
                'errInfo' => array('wtimeout' => true),
            );

            return $extractedError;
        }

        if ($code === self::ERR_NO_LONGER_PRIMARY ||
            $code === self::ERR_NO_LONGER_PRIMARY_REPLICATEDTONUM ||
            $code === self::ERR_GLE_WMODE_CHANGED_INVALID ||
            $code === self::ERR_NOT_MASTER ||
            $code === self::ERR_UNKNOWN_REPL_WRITE_CONCERN ||
            $code === self::ERR_WRITE_CONCERN_FAILED) {

            $extractedError['wcError'] = array(
                'code' => $code,
                'errmsg' => $errMsg,
            );

            return $extractedError;
        }

        if ( ! $isOk) {
            throw new UnexpectedValueException(sprintf('Unexpected error from getLastError: %s', json_encode($gle)));
        }

        if ($err !== '') {
            $extractedError['writeError'] = array(
                'code' => ($code === 0) ? self::ERR_UNKNOWN_ERROR : $code,
                'errmsg' => $err,
            );

            return $extractedError;
        }

        if ($jNote !== '') {
            $extractedError['writeError'] = array(
                'code' => self::ERR_WRITE_CONCERN_FAILED,
                'errmsg' => $jNote,
            );

            return $extractedError;
        }

        return $extractedError;
    }

    /**
     * Return an empty result array for execute().
     *
     * @return array
     */
    abstract protected function createEmptyResult();

    /**
     * Execute a single write operation.
     *
     * @param integer      $batchIndex Batch index of the write operation
     * @param array|object $document   Write operation
     * @param array        $result     Batch execution result (will be modified)
     */
    abstract protected function executeSingleOperation($batchIndex, $document, array &$result);
}
