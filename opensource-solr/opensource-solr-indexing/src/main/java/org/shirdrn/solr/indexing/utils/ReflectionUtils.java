package org.shirdrn.solr.indexing.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Class loading and instantiating utilities.
 * 
 * @author Yanjun
 */
public class ReflectionUtils {

	private static ClassLoader DEFAULT_CLASSLOADER = ReflectionUtils.class.getClassLoader();

	private static ClassLoader getClassLoader(ClassLoader classLoader) {
		if (classLoader == null) {
			classLoader = DEFAULT_CLASSLOADER;
		}
		return classLoader;
	}

	/**
	 * Instantiate a {@link Class} instance for given {@linkplain className}.
	 * 
	 * @param className
	 * @return
	 */
	public static Object getInstance(String className) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true,
					getClassLoader(DEFAULT_CLASSLOADER));
			instance = clazz.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class} instance for given {@linkplain className} and
	 * {@linkplain classLoader}.
	 * 
	 * @param className
	 * @param classLoader
	 * @return
	 */
	public static Object getInstance(String className, ClassLoader classLoader) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true,
					getClassLoader(classLoader));
			instance = clazz.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class} instance for given {@linkplain className}
	 * {@linkplain baseClass} and {@linkplain classLoader}.
	 * 
	 * @param className
	 * @param classLoader
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> T getInstance(String className, Class<T> baseClass,
			ClassLoader classLoader) {
		T instance = null;
		try {
			Class<?> clazz = Class.forName(className, true,
					getClassLoader(classLoader));
			instance = (T) clazz.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class} instance for given {@linkplain className}
	 * {@linkplain baseClass}, {@linkplain classLoader} and several
	 * {@linkplain parameters}.
	 * 
	 * @param className
	 * @param classLoader
	 * @return
	 */
	public static <T> T getInstance(String className, Class<T> baseClass,
			ClassLoader classLoader, Object... parameters) {
		T instance = null;
		try {
			Class<T> clazz = getClass(className, baseClass, classLoader);
			instance = construct(clazz, parameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class<T>} instance for given parameters:
	 * {@linkplain clazz} and {@linkplain parameters}.
	 * 
	 * @param <T>
	 * @param clazz
	 * @param parameters
	 * @return
	 */
	public static <T> T getInstance(Class<T> clazz, Object... parameters) {
		T instance = null;
		try {
			instance = construct(clazz, parameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	@SuppressWarnings("unchecked")
	private static <T> T construct(Class<T> clazz, Object... parameters)
			throws InstantiationException, IllegalAccessException,
			InvocationTargetException {
		T instance = null;
		Constructor<T>[] constructors = (Constructor<T>[]) clazz
				.getConstructors();
		for (Constructor<T> c : constructors) {
			if (c.getParameterTypes().length == parameters.length) {
				instance = c.newInstance(parameters);
				break;
			}
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class} instance from the given {@linkplain clazz}.
	 * 
	 * @param clazz
	 * @return
	 */
	public static <T> T getInstance(Class<T> clazz) {
		T instance = null;
		try {
			instance = clazz.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class} instance for given parameters:
	 * {@linkplain className} and {@linkplain parameters}, and default
	 * {@linkplain classLoader} is this {@link ReflectionUtils}'s class loader..
	 * 
	 * @param className
	 * @param parameters
	 * @return
	 */
	public static Object getInstance(String className, Object... parameters) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true,
					getClassLoader(DEFAULT_CLASSLOADER));
			instance = getInstance(clazz, parameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Instantiate a {@link Class} instance for given parameters:
	 * {@linkplain className}, {@linkplain parameters} and
	 * {@linkplain classLoader}.
	 * 
	 * @param className
	 * @param classLoader
	 * @param parameters
	 * @return
	 */
	public static Object getInstance(String className, ClassLoader classLoader,
			Object... parameters) {
		Object instance = null;
		try {
			Class<?> clazz = Class.forName(className, true,
					getClassLoader(classLoader));
			instance = getInstance(clazz, parameters);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return instance;
	}

	/**
	 * Get {@link Class} object based on the given {@linkplain className}, and
	 * {@linkplain baseClass}, default {@linkplain classLoader} is this
	 * {@link ReflectionUtils}'s class loader.
	 * 
	 * @param className
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Class<T> getClass(String className, Class<T> baseClass) {
		Class<T> clazz = null;
		try {
			clazz = (Class<T>) Class.forName(className, true,
					getClassLoader(DEFAULT_CLASSLOADER));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return clazz;
	}

	/**
	 * Get {@link Class} object based on the given {@linkplain className},
	 * {@linkplain baseClass} and {@linkplain classLoader}.
	 * 
	 * @param className
	 * @param baseClass
	 * @param classLoader
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public static <T> Class<T> getClass(String className, Class<T> baseClass,
			ClassLoader classLoader) {
		Class<T> clazz = null;
		try {
			clazz = (Class<T>) Class.forName(className, true,
					getClassLoader(classLoader));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return clazz;
	}

	public static Object getClass(String className, ClassLoader classLoader) {
		Object clazz = null;
		try {
			clazz = Class.forName(className, true, getClassLoader(classLoader));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return clazz;
	}

}