<?php

namespace MongoDB;

use UnexpectedValueException;

class Util
{
    const ERR_UNKNOWN_ERROR = 8;
    const ERR_WRITE_CONCERN_FAILED = 64;
    const ERR_UNKNOWN_REPL_WRITE_CONCERN = 79;
    const ERR_NOT_MASTER = 10107;

    /**
     * Parses a getLastError response and properly sets the write errors and
     * write concern errors.
     *
     * Should kept be up to date with BatchSafeWriter::extractGLEErrors in the
     * MongoDB server.
     *
     * @param array $gleResponse
     * @return array
     */
    public static function extractGLEErrors(array $gle)
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

        if ($code === 19900 || // No longer primary
            $code === 16805 || // replicatedToNum no longer primary
            $code === 14330 || // gle wmode changed; invalid
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
}
