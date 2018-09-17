<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor\Tests;

use Keboola\Component\UserException;
use Keboola\StorageApi\Client;
use Keboola\StorageExtractor\TokenValidator;
use PHPUnit\Framework\TestCase;

class TokenValidatorTest extends TestCase
{
    private function getClientMock(array $verifyTokenData): Client
    {
        $client = self::getMockBuilder(Client::class)
            ->disableOriginalConstructor()
            ->setMethods(['verifyToken'])
            ->getMock();
        $client->method('verifyToken')
            ->willReturn($verifyTokenData);
        /** @var Client $client */
        return $client;
    }

    public function testValidToken(): void
    {
        $client = $this->getClientMock(['bucketPermissions' => ['in.some-bucket' => 'read']]);
        $validator = new TokenValidator($client);
        self::assertSame('in.some-bucket', $validator->validate());
    }

    public function testTokenWrongPermissions(): void
    {
        $client = $this->getClientMock(['bucketPermissions' => ['in.some-bucket' => 'manage']]);
        $validator = new TokenValidator($client);
        self::expectExceptionMessage('The token must have read-only permissions to the bucket "in.some-bucket".');
        self::expectException(UserException::class);
        $validator->validate();
    }

    public function testTokenTooBroadPermissions(): void
    {
        $client = $this->getClientMock(
            ['bucketPermissions' => ['in.some-bucket' => 'read', 'out.another-bucket' => 'read']]
        );
        $validator = new TokenValidator($client);
        self::expectExceptionMessage('The token must have read-only permissions to a single bucket only.');
        self::expectException(UserException::class);
        $validator->validate();
    }

    public function testTokenTooNarrowPermissions(): void
    {
        $client = $this->getClientMock(['bucketPermissions' => []]);
        $validator = new TokenValidator($client);
        self::expectExceptionMessage('The token must have read-only permissions to a single bucket only.');
        self::expectException(UserException::class);
        $validator->validate();
    }
}
