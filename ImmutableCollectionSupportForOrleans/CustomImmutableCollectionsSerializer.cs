using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Orleans.Serialization;

namespace ImmutableCollectionSupportForOrleans
{
    [Orleans.CodeGeneration.RegisterSerializer()]
    internal class CustomImmutableSerializer
    {
        static CustomImmutableSerializer()
        {
            Register();
        }

        #region Immutable Dictionary
        internal static object GenericImmutableDictionaryDeepCopier(object original)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableDictionaryDeepCopier), nameof(ImmutableDictionarySerializer), nameof(ImmutableDictionaryDeserializer));
            return concreteMethods.Item3(original);
        }

        internal static object DeserializeGenericImmutableDictionary(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableDictionaryDeepCopier), nameof(ImmutableDictionarySerializer), nameof(ImmutableDictionaryDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static void SerializeGenericImmutableDictionary(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableDictionaryDeepCopier), nameof(ImmutableDictionarySerializer), nameof(ImmutableDictionaryDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }

        internal static object ImmutableDictionaryDeepCopier<K, V>(object original)
        {
            return original;
        }

        internal static void ImmutableDictionarySerializer<K, V>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var dict = (ImmutableDictionary<K, V>)untypedInput;
            SerializationManager.SerializeInner(dict.KeyComparer.Equals(EqualityComparer<K>.Default) ? null : dict.KeyComparer, stream, typeof(IEqualityComparer<K>));
            SerializationManager.SerializeInner(dict.ValueComparer.Equals(EqualityComparer<V>.Default) ? null : dict.ValueComparer, stream, typeof(IEqualityComparer<V>));

            stream.Write(dict.Count);
            foreach (var pair in dict)
            {
                SerializationManager.SerializeInner(pair.Key, stream, typeof(K));
                SerializationManager.SerializeInner(pair.Value, stream, typeof(V));
            }
        }

        internal static object ImmutableDictionaryDeserializer<K, V>(Type expected, BinaryTokenStreamReader stream)
        {
            var keyComparer = SerializationManager.DeserializeInner<IEqualityComparer<K>>(stream);
            var valueComparer = SerializationManager.DeserializeInner<IEqualityComparer<V>>(stream);
            var count = stream.ReadInt();
            var dictBuilder = ImmutableDictionary.CreateBuilder(keyComparer, valueComparer);
            for (var i = 0; i < count; i++)
            {
                var key = SerializationManager.DeserializeInner<K>(stream);
                var value = SerializationManager.DeserializeInner<V>(stream);
                dictBuilder.Add(key, value);
            }
            var dict = dictBuilder.ToImmutable();
            DeserializationContext.Current.RecordObject(dict);

            return dict;
        }
        #endregion

        #region Immutable List
        internal static object ImmutableListDeepCopier<K>(object original)
        {
            return original;
        }

        internal static void ImmutableListSerializer<T>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var list = (ImmutableList<T>)untypedInput;
            stream.Write(list.Count);
            foreach (var element in list)
            {
                SerializationManager.SerializeInner(element, stream, typeof(T));
            }
        }

        internal static object ImmutableListDeserializer<T>(Type expected, BinaryTokenStreamReader stream)
        {
            var count = stream.ReadInt();
            var listBuilder = ImmutableList.CreateBuilder<T>();

            for (var i = 0; i < count; i++)
            {
                listBuilder.Add(SerializationManager.DeserializeInner<T>(stream));
            }
            var list = listBuilder.ToImmutable();
            DeserializationContext.Current.RecordObject(list);
            return list;
        }

        internal static void SerializeGenericImmutableList(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableListDeepCopier), nameof(ImmutableListSerializer), nameof(ImmutableListDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }
        internal static object DeserializeGenericImmutableList(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableListDeepCopier), nameof(ImmutableListSerializer), nameof(ImmutableListDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static object GenericImmutableListDeepCopier(object original)
        {
            var t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableListDeepCopier), nameof(ImmutableListSerializer), nameof(ImmutableListDeserializer));
            return concreteMethods.Item3(original);
        }
        #endregion

        #region Immutable HashSet
        internal static object GenericImmutableHashSetDeepCopier(object original)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableHashSetDeepCopier), nameof(ImmutableHashSetSerializer), nameof(ImmutableHashSetDeserializer));
            return concreteMethods.Item3(original);
        }

        internal static object DeserializeGenericImmutableHashSet(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableHashSetDeepCopier), nameof(ImmutableHashSetSerializer), nameof(ImmutableHashSetDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static void SerializeGenericImmutableHashSet(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableHashSetDeepCopier), nameof(ImmutableHashSetSerializer), nameof(ImmutableHashSetDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }

        internal static object ImmutableHashSetDeepCopier<K>(object original)
        {
            return original;
        }

        internal static void ImmutableHashSetSerializer<K>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var dict = (ImmutableHashSet<K>)untypedInput;
            SerializationManager.SerializeInner(dict.KeyComparer.Equals(EqualityComparer<K>.Default) ? null : dict.KeyComparer, stream, typeof(IEqualityComparer<K>));

            stream.Write(dict.Count);
            foreach (var pair in dict)
            {
                SerializationManager.SerializeInner(pair, stream, typeof(K));
            }
        }

        internal static object ImmutableHashSetDeserializer<K>(Type expected, BinaryTokenStreamReader stream)
        {
            var keyComparer = SerializationManager.DeserializeInner<IEqualityComparer<K>>(stream);
            var count = stream.ReadInt();
            var dictBuilder = ImmutableHashSet.CreateBuilder(keyComparer);
            for (var i = 0; i < count; i++)
            {
                var key = SerializationManager.DeserializeInner<K>(stream);
                dictBuilder.Add(key);
            }
            var dict = dictBuilder.ToImmutable();
            DeserializationContext.Current.RecordObject(dict);

            return dict;
        }
        #endregion

        #region Sorted Set
        internal static object GenericImmutableSortedSetDeepCopier(object original)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableSortedSetDeepCopier), nameof(ImmutableSortedSetSerializer), nameof(ImmutableSortedSetDeserializer));
            return concreteMethods.Item3(original);
        }

        internal static object DeserializeGenericImmutableSortedSet(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableSortedSetDeepCopier), nameof(ImmutableSortedSetSerializer), nameof(ImmutableSortedSetDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static void SerializeGenericImmutableSortedSet(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableSortedSetDeepCopier), nameof(ImmutableSortedSetSerializer), nameof(ImmutableSortedSetDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }

        internal static object ImmutableSortedSetDeepCopier<K>(object original)
        {
            return original;
        }

        internal static void ImmutableSortedSetSerializer<K>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var dict = (ImmutableSortedSet<K>)untypedInput;
            SerializationManager.SerializeInner(dict.KeyComparer.Equals(Comparer<K>.Default) ? null : dict.KeyComparer, stream, typeof(IComparer<K>));

            stream.Write(dict.Count);
            foreach (var pair in dict)
            {
                SerializationManager.SerializeInner(pair, stream, typeof(K));
            }
        }

        internal static object ImmutableSortedSetDeserializer<K>(Type expected, BinaryTokenStreamReader stream)
        {
            var keyComparer = SerializationManager.DeserializeInner<IComparer<K>>(stream);
            var count = stream.ReadInt();
            var dictBuilder = ImmutableSortedSet.CreateBuilder(keyComparer);
            for (var i = 0; i < count; i++)
            {
                var key = SerializationManager.DeserializeInner<K>(stream);
                dictBuilder.Add(key);
            }
            var dict = dictBuilder.ToImmutable();
            DeserializationContext.Current.RecordObject(dict);

            return dict;
        }
        #endregion

        #region Sorted Dictionary 
        internal static object GenericImmutableSortedDictionaryDeepCopier(object original)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableSortedDictionaryDeepCopier), nameof(ImmutableSortedDictionarySerializer), nameof(ImmutableSortedDictionaryDeserializer));
            return concreteMethods.Item3(original);
        }

        internal static object DeserializeGenericImmutableSortedDictionary(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableSortedDictionaryDeepCopier), nameof(ImmutableSortedDictionarySerializer), nameof(ImmutableSortedDictionaryDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static void SerializeGenericImmutableSortedDictionary(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableSortedDictionaryDeepCopier), nameof(ImmutableSortedDictionarySerializer), nameof(ImmutableSortedDictionaryDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }

        internal static object ImmutableSortedDictionaryDeepCopier<K, V>(object original)
        {
            return original;
        }

        internal static void ImmutableSortedDictionarySerializer<K, V>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var dict = (ImmutableSortedDictionary<K, V>)untypedInput;
            SerializationManager.SerializeInner(dict.KeyComparer.Equals(Comparer<K>.Default) ? null : dict.KeyComparer, stream, typeof(IComparer<K>));
            SerializationManager.SerializeInner(dict.ValueComparer.Equals(EqualityComparer<V>.Default) ? null : dict.ValueComparer, stream, typeof(IEqualityComparer<V>));

            stream.Write(dict.Count);
            foreach (var pair in dict)
            {
                SerializationManager.SerializeInner(pair.Key, stream, typeof(K));
                SerializationManager.SerializeInner(pair.Value, stream, typeof(V));
            }
        }

        internal static object ImmutableSortedDictionaryDeserializer<K, V>(Type expected, BinaryTokenStreamReader stream)
        {
            var keyComparer = SerializationManager.DeserializeInner<IComparer<K>>(stream);
            var valueComparer = SerializationManager.DeserializeInner<IEqualityComparer<V>>(stream);
            var count = stream.ReadInt();
            var dictBuilder = ImmutableSortedDictionary.CreateBuilder(keyComparer, valueComparer);
            for (var i = 0; i < count; i++)
            {
                var key = SerializationManager.DeserializeInner<K>(stream);
                var value = SerializationManager.DeserializeInner<V>(stream);
                dictBuilder.Add(key, value);
            }
            var dict = dictBuilder.ToImmutable();
            DeserializationContext.Current.RecordObject(dict);

            return dict;
        }
        #endregion

        #region Immutable Array
        internal static object GenericImmutableArrayDeepCopier(object original)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableArrayDeepCopier), nameof(ImmutableArraySerializer), nameof(ImmutableArrayDeserializer));
            return concreteMethods.Item3(original);
        }

        internal static object DeserializeGenericImmutableArray(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableArrayDeepCopier), nameof(ImmutableArraySerializer), nameof(ImmutableArrayDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static void SerializeGenericImmutableArray(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableArrayDeepCopier), nameof(ImmutableArraySerializer), nameof(ImmutableArrayDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }

        internal static object ImmutableArrayDeepCopier<K>(object original)
        {
            return original;
        }

        internal static void ImmutableArraySerializer<K>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var dict = (ImmutableArray<K>)untypedInput;

            stream.Write(dict.Length);
            foreach (var pair in dict)
            {
                SerializationManager.SerializeInner(pair, stream, typeof(K));
            }
        }

        internal static object ImmutableArrayDeserializer<K>(Type expected, BinaryTokenStreamReader stream)
        {
            var count = stream.ReadInt();
            var dictBuilder = ImmutableArray.CreateBuilder<K>();
            for (var i = 0; i < count; i++)
            {
                var key = SerializationManager.DeserializeInner<K>(stream);
                dictBuilder.Add(key);
            }
            var dict = dictBuilder.ToImmutable();
            DeserializationContext.Current.RecordObject(dict);

            return dict;
        }
        #endregion

        #region Immutable Queue
        internal static object GenericImmutableQueueDeepCopier(object original)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableQueueDeepCopier), nameof(ImmutableQueueSerializer), nameof(ImmutableQueueDeserializer));
            return concreteMethods.Item3(original);
        }

        internal static object DeserializeGenericImmutableQueue(Type expected, BinaryTokenStreamReader stream)
        {
            var concreteMethods = RegisterConcreteMethods(expected, typeof(CustomImmutableSerializer),
                nameof(ImmutableQueueDeepCopier), nameof(ImmutableQueueSerializer), nameof(ImmutableQueueDeserializer));
            return concreteMethods.Item2(expected, stream);
        }

        internal static void SerializeGenericImmutableQueue(object original, BinaryTokenStreamWriter stream, Type expected)
        {
            Type t = original.GetType();
            var concreteMethods = RegisterConcreteMethods(t, typeof(CustomImmutableSerializer),
                nameof(ImmutableQueueDeepCopier), nameof(ImmutableQueueSerializer), nameof(ImmutableQueueDeserializer));
            concreteMethods.Item1(original, stream, expected);
        }

        internal static object ImmutableQueueDeepCopier<K>(object original)
        {
            return original;
        }

        internal static void ImmutableQueueSerializer<K>(object untypedInput, BinaryTokenStreamWriter stream, Type typeExpected)
        {
            var queue = (ImmutableQueue<K>)untypedInput;

            stream.Write(queue.Count());
            foreach (var item in queue)
            {
                SerializationManager.SerializeInner(item, stream, typeof(K));
            }
        }

        internal static object ImmutableQueueDeserializer<K>(Type expected, BinaryTokenStreamReader stream)
        {
            var count = stream.ReadInt();
            var items = new K[count];
            for (var i = 0; i < count; i++)
            {
                var key = SerializationManager.DeserializeInner<K>(stream);
                items[i] = key;
            }
            var queues = ImmutableQueue.CreateRange(items);

            DeserializationContext.Current.RecordObject(queues);

            return queues;
        }
        #endregion

        public static void Register()
        {
            SerializationManager.Register(typeof(ImmutableQueue<>), GenericImmutableQueueDeepCopier, SerializeGenericImmutableQueue, DeserializeGenericImmutableQueue);
            SerializationManager.Register(typeof(ImmutableArray<>), GenericImmutableArrayDeepCopier, SerializeGenericImmutableArray, DeserializeGenericImmutableArray);
            SerializationManager.Register(typeof(ImmutableSortedDictionary<,>), GenericImmutableSortedDictionaryDeepCopier, SerializeGenericImmutableSortedDictionary, DeserializeGenericImmutableSortedDictionary);
            SerializationManager.Register(typeof(ImmutableSortedSet<>), GenericImmutableSortedSetDeepCopier, SerializeGenericImmutableSortedSet, DeserializeGenericImmutableSortedSet);
            SerializationManager.Register(typeof(ImmutableHashSet<>), GenericImmutableHashSetDeepCopier, SerializeGenericImmutableHashSet, DeserializeGenericImmutableHashSet);
            SerializationManager.Register(typeof(ImmutableDictionary<,>), GenericImmutableDictionaryDeepCopier, SerializeGenericImmutableDictionary, DeserializeGenericImmutableDictionary);
            SerializationManager.Register(typeof(ImmutableList<>), GenericImmutableListDeepCopier, SerializeGenericImmutableList, DeserializeGenericImmutableList);
        }

        public static Tuple<SerializationManager.Serializer, SerializationManager.Deserializer, SerializationManager.DeepCopier>
            RegisterConcreteMethods(Type concreteType, Type definingType, string copierName, string serializerName, string deserializerName, Type[] genericArgs = null)
        {
            if (genericArgs == null)
            {
                genericArgs = concreteType.GetGenericArguments();
            }

            var genericCopier = definingType.GetMethods(BindingFlags.Static | BindingFlags.NonPublic).Where(m => m.Name == copierName).FirstOrDefault();
            var concreteCopier = genericCopier.MakeGenericMethod(genericArgs);
            var copier = (SerializationManager.DeepCopier)concreteCopier.CreateDelegate(typeof(SerializationManager.DeepCopier));

            var genericSerializer = definingType.GetMethods(BindingFlags.Static | BindingFlags.NonPublic).Where(m => m.Name == serializerName).FirstOrDefault();
            var concreteSerializer = genericSerializer.MakeGenericMethod(genericArgs);
            var serializer = (SerializationManager.Serializer)concreteSerializer.CreateDelegate(typeof(SerializationManager.Serializer));

            var genericDeserializer = definingType.GetMethods(BindingFlags.Static | BindingFlags.NonPublic).Where(m => m.Name == deserializerName).FirstOrDefault();
            var concreteDeserializer = genericDeserializer.MakeGenericMethod(genericArgs);
            var deserializer =
                (SerializationManager.Deserializer)concreteDeserializer.CreateDelegate(typeof(SerializationManager.Deserializer));

            SerializationManager.Register(concreteType, copier, serializer, deserializer);

            return new Tuple<SerializationManager.Serializer, SerializationManager.Deserializer, SerializationManager.DeepCopier>(serializer, deserializer, copier);
        }

    }
}
