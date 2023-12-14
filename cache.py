import numpy as np
import redis


class Redis(redis.Redis):
    def __init__(self, host, port, password=""):
        super().__init__(host=str(host), port=int(port), password=password)
        assert self.ping()
        self.flushall()
        print(f"Connected to Redis-server")

    def setArray(self, key, array: np.ndarray) -> None:
        key = str(key)
        shape = ",".join([str(x) for x in array.shape])
        dtype = str(array.dtype)
        encoded_array = array.tobytes()

        self.set(f"{key}_shape", shape)
        self.set(f"{key}_dtype", dtype)
        self.set(f"{key}_data", encoded_array)

    def getArray(self, key) -> np.ndarray:
        key = str(key)
        
        shape = self.get(f"{key}_shape")
        dtype = self.get(f"{key}_dtype")
        encoded_array = self.get(f"{key}_data")
        
        if shape and dtype and encoded_array:
            shape = [int(c) for c in shape.split(b",")]
            decoded_data = np.frombuffer(encoded_array, dtype=dtype)
            return np.reshape(decoded_data, shape)
        else:
            return None
