import copyreg
import sys
import types
from itertools import groupby
from itertools import islice
from typing import Tuple, Dict, Iterable, Callable, Set

from ipystate.impl.utils import check_object_importable_by_name, SAVE_GLOBAL, reduce_type

WALK_SUBTREE_LIMIT = 1000


class Walker:
    def __init__(self, logger=None, dispatch_table=None):
        self._logger = logger
        self._constant = object()

        self._object_labels = None
        self._memo = None

        self._labels_found = None
        self._current_label = None
        self._full_walk = False
        self._current_subtree_size = 0

        if dispatch_table is None:
            dispatch_table = copyreg.dispatch_table.copy()
        self._dispatch_table = dispatch_table

    def enable_full_walk(self):
        self._full_walk = True

    def disable_full_walk(self):
        self._full_walk = False

    def walk(self, env: Dict[str, object]) -> Iterable[Set[str]]:
        if 'numpy' in sys.modules:
            import numpy

            self.dispatch[numpy.dtype] = self.save_constant

        self._object_labels = {}
        self._memo = {}

        label_sets = []
        for name in env.keys():
            self._labels_found = {name}
            self._current_label = name
            self._current_subtree_size = 0
            try:
                self._save(env[name])
            except Exception as e:
                self._error(f"Walker: could not walk through variable {name} of type {type(env[name])}")
                self._error(f"Error: {str(e)}")
            finally:
                label_sets.append(self._labels_found)

        # keep names that are presented in env
        for label_set in label_sets:
            for label in label_set.copy():
                if label not in env:
                    label_set.remove(label)

        # unite components
        for i, labels in enumerate(label_sets):
            for j, lab in enumerate(islice(label_sets, 0, i)):
                if labels & lab:
                    labels.update(lab)
                    label_sets[j] = labels
        clusters = [frozenset(s) for s, _ in groupby(sorted(label_sets, key=id))]

        if self._object_labels is not None:
            self._object_labels.clear()
        if self._memo is not None:
            self._memo.clear()
        if self._labels_found is not None:
            self._labels_found.clear()

        # noinspection PyTypeChecker
        return clusters

    def _error(self, msg: str) -> None:
        if self._logger is not None:
            self._logger.error(msg)

    def _was_visited(self, obj: object) -> bool:
        return id(obj) in self._object_labels

    def _save(self, obj: object) -> None:
        if not self._full_walk and self._current_subtree_size > WALK_SUBTREE_LIMIT:
            raise Exception('walk depth limit exceeded')
        self._current_subtree_size += 1

        assert self._labels_found is not None
        was_visited = self._was_visited(obj)
        self._visit_object(obj)

        if was_visited:
            return None

        if obj is type(None) or obj is type(NotImplemented) or obj is type(...):
            self._unvisit_object(obj)
            return None

        # visit with dispatch table
        t = type(obj)
        f = self.dispatch.get(t)
        if f is not None:
            # noinspection PyArgumentList
            result = f(self, obj)  # Call unbound method with explicit self
            if result == self._constant:
                self._unvisit_object(obj)
            return result

        # check copyreg.dispatch_table
        reduce = self._dispatch_table.get(t)
        if reduce is None and issubclass(t, type):
            reduce = reduce_type

        if reduce is not None:
            # noinspection PyTypeChecker
            rv = reduce(obj)
        else:
            # Check for a __reduce_ex__ method, fall back to __reduce__
            reduce = getattr(obj, "__reduce_ex__", None)
            if reduce is not None:
                rv = reduce(3)
            else:
                reduce = getattr(obj, "__reduce__", None)
                if reduce is None:
                    raise Exception("Can't reduce {!r} object: {!r}".format(t.__name__, obj))
                rv = reduce()

        if rv is SAVE_GLOBAL:
            self._unvisit_object(obj)
            self._save_global(obj)
            return None

        # Check for string returned by reduce(), meaning "save as global"
        if isinstance(rv, str):
            self._unvisit_object(obj)
            self._save_global(obj, rv)
            return None

        # Assert that reduce() returned a tuple
        if not isinstance(rv, tuple):
            raise Exception('{!r} must return string or tuple'.format(reduce))

        # Assert that it returned an appropriately sized tuple
        length = len(rv)
        if not (2 <= length <= 5):
            raise Exception('Tuple returned by {!r} must have two to five elements'.format(reduce))

        # Save the reduce() output and finally memoize the object
        if self._save_reduce(obj=obj, *rv) == self._constant:
            self._unvisit_object(obj)

    def _visit_object(self, obj: object) -> None:
        labels = self._object_labels.setdefault(id(obj), self._labels_found)
        self._labels_found.update(labels)
        labels.add(self._current_label)
        if isinstance(obj, types.CodeType):
            labels.update(obj.co_names)

    def _unvisit_object(self, obj: object) -> None:
        del self._object_labels[id(obj)]

    def _save_reduce(self, func: Callable, args: Tuple, state=None, listitems=None, dictitems=None, obj=None) -> None:
        if not isinstance(args, tuple):
            raise Exception('args from save_reduce() must be a tuple')
        if not callable(func):
            raise Exception('func from save_reduce() must be callable')

        func_name = getattr(func, "__name__", "")
        if func_name == "__newobj__":
            # Commented by tomato start
            # cls = args[0]
            # if not hasattr(cls, "__new__"):
            #     raise Exception('args[0] from __newobj__ args has no __new__')
            # if obj is not None and cls is not obj.__class__:
            #     raise Exception('args[0] from __newobj__ args has the wrong class')
            # cls_result = self._save(cls)
            # Commented by tomato end

            args = args[1:]
            self._save(args)
        else:
            self._save(func)
            self._save(args)

        if obj is not None:
            if id(obj) not in self._memo:
                self._memoize(obj)
            else:
                pass
                # recursive

        # More new special cases (that work with older protocols as
        # well): when __reduce__ returns a tuple with 4 or 5 items,
        # the 4th and 5th item should be iterators that provide list
        # items and dict items (as (key, value) tuples), or None.

        if listitems is not None:
            self._batch_appends(listitems)

        if dictitems is not None:
            self._batch_setitems(dictitems)

        if state is not None:
            self._save(state)

    # Methods below this point are dispatched through the dispatch table

    dispatch = {}

    def save_constant(self, _) -> object:
        return self._constant

    dispatch[type(None)] = save_constant
    dispatch[bool] = save_constant
    dispatch[int] = save_constant
    dispatch[float] = save_constant
    dispatch[bytes] = save_constant
    dispatch[str] = save_constant

    def _save_tuple(self, obj: Tuple) -> object:
        if not obj:  # tuple is empty
            return self._constant

        all_constants = True
        for element in obj:
            if self._save(element) != self._constant:
                all_constants = False

        if all_constants:
            # todo save constant property in memo
            return self._constant

        if id(obj) in self._memo:
            # recursive tuple
            return

        # No recursion
        self._memoize(obj)

    dispatch[tuple] = _save_tuple

    def _save_list(self, obj) -> None:
        self._memoize(obj)
        if self._should_stop_walking(obj, len(obj)):
            return
        for x in obj:
            self._save(x)

    dispatch[list] = _save_list

    def _batch_appends(self, items) -> None:
        for x in items:
            self._save(x)

    def _save_dict(self, obj) -> None:
        self._memoize(obj)
        if self._should_stop_walking(obj, len(obj)):
            return
        self._batch_setitems(obj.items())

    dispatch[dict] = _save_dict

    def _batch_setitems(self, items) -> None:
        for k, v in items:
            self._save(k)
            self._save(v)

    def _save_set(self, obj) -> None:
        self._memoize(obj)
        if self._should_stop_walking(obj, len(obj)):
            return
        for item in obj:
            self._save(item)

    dispatch[set] = _save_set

    def _save_frozenset(self, obj) -> None:
        self._memoize(obj)
        if self._should_stop_walking(obj, len(obj)):
            return
        for item in obj:
            self._save(item)

    dispatch[frozenset] = _save_frozenset

    def _save_global(self, obj, name: str = None) -> None:
        result = check_object_importable_by_name(obj, name)
        if result is None:
            raise Exception("Can't save_global {!r}, type: {}: object is not importable by name".format(obj, type(obj)))
        name, module_name, module, parent = result

        # noinspection PyProtectedMember
        code = copyreg._extension_registry.get((module_name, name))
        if code:
            assert code > 0
            return

        lastname = name.rpartition('.')[2]
        if parent is module:
            name = lastname

        self._save(module_name)
        self._save(name)

        self._memoize(obj)

    def _memoize(self, obj: object) -> None:
        """Store an object in the memo."""

        # The Pickler memo is a dictionary mapping object ids to 2-tuples
        # that contain the Unpickler memo key and the object being memoized.
        # The memo key is written to the pickle and will become
        # the key in the Unpickler's memo.  The object is stored in the
        # Pickler memo so that transient objects are kept alive during
        # pickling.

        # The use of the Unpickler memo length as the memo key is just a
        # convention.  The only requirement is that the memo values be unique.
        # But there appears no advantage to any other scheme, and this
        # scheme allows the Unpickler memo to be implemented as a plain (but
        # growable) array, indexed by memo key.
        # assert id(obj) not in self._memo
        self._memo[id(obj)] = obj

    def _should_stop_walking(self, obj, size: int) -> bool:
        if not self._full_walk and self._current_subtree_size + size > WALK_SUBTREE_LIMIT:
            if self._logger is not None:
                self._logger.warn('Skipping walk through ' + str(type(obj)) + ' with size: ' + str(size))
            return True
        return False
