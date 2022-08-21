package cluster

import (
	"context"
	"errors"
	"redisgo/redis/client"

	pool "github.com/jolestar/go-commons-pool/v2"
)

type connectionFactory struct {
	Peer string //连接池连接的peer database

}

func (f *connectionFactory) MakeObject(ctx context.Context) (*pool.PooledObject, error) {
	c, err := client.MakeClient(f.Peer)
	if err != nil {
		return nil, err
	}
	c.Start()
	cc := pool.NewPooledObject(c) //新建客户端对象，放入池中
	return cc, nil
}

/**
 * Destroys an instance no longer needed by the pool.
 */
func (f *connectionFactory) DestroyObject(ctx context.Context, object *pool.PooledObject) error {
	c, ok := object.Object.(*client.Client)
	if !ok {
		return errors.New("type mismatch")
	}
	c.Close()
	return nil
}

/**
 * Ensures that the instance is safe to be returned by the pool.
 *
 * return false if object is not valid and should
 *         be dropped from the pool, true otherwise.
 */
func (f *connectionFactory) ValidateObject(ctx context.Context, object *pool.PooledObject) bool {
	return true
}

/**
 * Reinitialize an instance to be returned by the pool.
 *
 * return error if there is a problem activating object,
 *    this error may be swallowed by the pool.
 */
func (f *connectionFactory) ActivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}

/**
 * Uninitialize an instance to be returned to the idle object pool.
 *
 * return error if there is a problem passivating obj,
 *    this exception may be swallowed by the pool.
 */
func (f *connectionFactory) PassivateObject(ctx context.Context, object *pool.PooledObject) error {
	return nil
}