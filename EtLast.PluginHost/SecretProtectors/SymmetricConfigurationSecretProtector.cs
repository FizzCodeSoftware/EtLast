namespace FizzCode.EtLast.PluginHost
{
    using System;
    using System.IO;
    using System.Security.Cryptography;
    using System.Text;
    using FizzCode.LightWeight.Configuration;
    using Microsoft.Extensions.Configuration;

    public class SymmetricConfigurationSecretProtector : IConfigurationSecretProtector
    {
        private byte[] _key;
        private byte[] _iv;

        public bool Init(IConfigurationSection configurationSection)
        {
            var baseKey = ConfigurationReader.GetCurrentValue<string>(configurationSection, "BaseKey", null);
            if (string.IsNullOrEmpty(baseKey))
                return false;

            var useMachineName = ConfigurationReader.GetCurrentValue(configurationSection, "UseMachineName", false);
            var useUserName = ConfigurationReader.GetCurrentValue(configurationSection, "UseUserName", false);

            var pw = baseKey;
            if (!string.IsNullOrEmpty(baseKey))
            {
                try
                {
                    pw = baseKey.Trim()
                        + (useMachineName ? "\0" + Environment.MachineName : "")
                        + (useUserName ? "\0" + Environment.UserName : "");
                }
                catch (Exception)
                {
                    return false;
                }
            }

            using (var hash = SHA256.Create())
            {
                _key = hash.ComputeHash(Encoding.UTF8.GetBytes(pw));
            }

            _iv = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5 };

            return true;
        }

        public string Decrypt(string value)
        {
            var inputBytes = Convert.FromBase64String(value);

            using (var algo = Rijndael.Create())
            {
                algo.Key = _key;
                algo.IV = _iv;

                using (var decryptor = algo.CreateDecryptor(algo.Key, algo.IV))
                using (var memoryStream = new MemoryStream(inputBytes))
                using (var decryptorStream = new CryptoStream(memoryStream, decryptor, CryptoStreamMode.Read))
                using (var srDecrypt = new StreamReader(decryptorStream))
                {
                    return srDecrypt.ReadToEnd();
                }
            }
        }

        public string Encrypt(string value)
        {
            var inputBytes = Encoding.UTF8.GetBytes(value);

            using (var algo = Rijndael.Create())
            {
                algo.Key = _key;
                algo.IV = _iv;

                using (var encryptor = algo.CreateEncryptor())
                using (var resultStream = new MemoryStream())
                using (var encryptedStream = new CryptoStream(resultStream, encryptor, CryptoStreamMode.Write))
                {
                    encryptedStream.Write(inputBytes);
                    encryptedStream.FlushFinalBlock();
                    return Convert.ToBase64String(resultStream.ToArray());
                }
            }
        }
    }
}