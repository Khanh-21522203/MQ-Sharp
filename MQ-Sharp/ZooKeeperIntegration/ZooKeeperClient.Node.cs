using MQ_Sharp.Utils;
using ZooKeeperNet;

namespace MQ_Sharp.ZooKeeperIntegration;

public partial class ZooKeeperClient
{
    private readonly IZooKeeperSerializer serializer;

    #region Nodes
    public void CreatePersistent(string path, bool createParents)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        try
        {
            Create(path, null, CreateMode.Persistent);
        }

        catch (KeeperException e)
        {
            if (e.ErrorCode == KeeperException.Code.NODEEXISTS)
            {
                if (!createParents)
                {
                    throw;
                }
            }
            else if (e.ErrorCode == KeeperException.Code.NONODE)
            {
                if (!createParents)
                {
                    throw;
                }

                string parentDir = path.Substring(0, path.LastIndexOf('/'));
                CreatePersistent(parentDir, true);
                CreatePersistent(path, true);
            }
            else
                throw;
        }
    }

    public void CreatePersistent(string path)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        CreatePersistent(path, false);
    }
    public void CreatePersistent(string path, object data)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        Create(path, data, CreateMode.Persistent);
    }
    public string CreatePersistentSequential(string path, object data)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        return Create(path, data, CreateMode.PersistentSequential);
    }
    
    public void CreateEphemeral(string path)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        Create(path, null, CreateMode.Ephemeral);
    }
    public void CreateEphemeral(string path, object data)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        Create(path, data, CreateMode.Ephemeral);
    }
    public string CreateEphemeralSequential(string path, object data)
    {
        Guard.NotNullNorEmpty(path, "path");
        EnsuresNotDisposed();
        return Create(path, data, CreateMode.EphemeralSequential);
    }
    
    private string Create(string path, object data, CreateMode mode)
    {
        if (path == null)
        {
            throw new ArgumentNullException("Path must not be null");
        }

        byte[] bytes = data == null ? null : serializer.Serialize(data);
        return RetryUntilConnected(() => connection.Create(path, bytes, mode));
    }
    #endregion
    
    #region Delete

    public bool Delete(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        return RetryUntilConnected(
            () =>
            {
                try
                {
                    connection.Delete(path);
                    return true;
                }
                catch (KeeperException e)// KeeperException.NoNodeException)
                {
                    if (e.ErrorCode == KeeperException.Code.NONODE)
                        return false;
                    else
                        throw;
                }
            });
    }

    public bool DeleteRecursive(string path)
    {
        Guard.NotNullNorEmpty(path, "path");

        EnsuresNotDisposed();
        IEnumerable<string> children;
        try
        {
            children = GetChildren(path, false);
        }
        catch (KeeperException e)
        {
            if (e.ErrorCode == KeeperException.Code.NONODE)
                return true;
            else
                throw;
        }

        foreach (var child in children)
        {
            if (!DeleteRecursive(path + "/" + child))
            {
                return false;
            }
        }

        return Delete(path);
    }
    #endregion
}