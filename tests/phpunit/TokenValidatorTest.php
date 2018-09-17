<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor\Tests;

use Keboola\Component\UserException;
use Keboola\StorageApi\Client;
use Keboola\StorageExtractor\TokenValidator;
use PHPUnit\Framework\TestCase;

class TokenValidatorTest extends TestCase
{
    public function testValidToken(): void
    {
        $client = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['verifyToken'])
            ->getMock();
        $client->method('verifyToken')
            ->willReturn(['bucketPermissions' => ['in.some-bucket' => 'read']]);
        /** @var Client $client */
        $validator = new TokenValidator($client);
        self::assertSame('in.some-bucket', $validator->validate());
    }

    public function testTokenWrongPermissions(): void
    {
        $client = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['verifyToken'])
            ->getMock();
        $client->method('verifyToken')
            ->willReturn(['bucketPermissions' => ['in.some-bucket' => 'manage']]);
        /** @var Client $client */
        $validator = new TokenValidator($client);
        self::expectExceptionMessage('The token must have read-only permissions to the bucket "in.some-bucket".');
        self::expectException(UserException::class);
        $validator->validate();
    }

    public function testTokenTooBroadPermissions(): void
    {
        $client = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['verifyToken'])
            ->getMock();
        $client->method('verifyToken')
            ->willReturn(['bucketPermissions' => ['in.some-bucket' => 'read', 'out.another-bucket' => 'read']]);
        /** @var Client $client */
        $validator = new TokenValidator($client);
        self::expectExceptionMessage('The token must have read-only permissions to a single bucket only.');
        self::expectException(UserException::class);
        $validator->validate();
    }
}
