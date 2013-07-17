<?php

namespace MongoDB\Exception;

use RuntimeException;

/**
 * ResultException is thrown when a database command fails.
 *
 * This is similar to the driver's MongoResultException class, which cannot be
 * be used due to inaccessibility of its result document property.
 *
 * @see http://php.net/manual/en/class.mongoresultexception.php
 */
class ResultException extends RuntimeException
{
    /**
     * The command result document.
     *
     * @var array
     */
    private $document;

    /**
     * Constructor.
     *
     * @param array $document Command result document
     */
    public function __construct(array $document)
    {
        $message = isset($result['errmsg']) ? $result['errmsg'] : 'Unknown error executing command';
        $code = isset($result['code']) ? $result['code'] : 0;

        parent::__construct($message, $code);
    }

    /**
     * Get the command result document.
     *
     * @return array
     */
    public function getDocument()
    {
        return $this->document;
    }
}
