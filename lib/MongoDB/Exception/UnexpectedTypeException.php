<?php

namespace MongoDB\Exception;

use RuntimeException;

/**
 * UnexpectedTypeException is thrown when a value's type is unexpected.
 */
class UnexpectedTypeException extends RuntimeException
{
    public function __construct($value, $expectedType)
    {
        parent::__construct(sprintf('Expected argument of type "%s", "%s" given', $expectedType, is_object($value) ? get_class($value) : gettype($value)));
    }
}
