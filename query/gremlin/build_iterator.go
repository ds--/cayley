// Copyright 2014 The Cayley Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gremlin

import (
	"strconv"

	"github.com/barakmich/glog"
	"github.com/robertkrimen/otto"

	"github.com/google/cayley/graph"
	"github.com/google/cayley/graph/iterator"
	"github.com/google/cayley/quad"
)

func propertiesOf(obj *otto.Object, name string) []string {
	val, _ := obj.Get(name)
	if val.IsUndefined() {
		return nil
	}
	export, _ := val.Export()
	return export.([]string)
}

// get a property as an array of values
func propertyAsArray(obj *otto.Object, name string) []otto.Value {
	valuesList, err := obj.Get(name)
	if err != nil {
		return nil
	}
	goValuesList, exerr := valuesList.Export()
	if exerr != nil {
		return nil
	}
	return goValuesList.([]otto.Value)
}

// convert an otto array of js values into an
// array of go strings, single values are converted into
// an array with a single element
func convertOttoObjToStrArray(val otto.Value) []string {
	var strarr []string
	if val.IsString() {
		strarr = append(strarr, val.String())
	} else {
		if val.Class() == "Array" {
			nativeVal, err := val.Export()
			if err != nil {
				glog.Errorln("Failed to export JS object.")
				return nil
			}
			for _, predicate := range nativeVal.([]interface{}) {
				switch v := predicate.(type) {
				case string:
					strarr = append(strarr, v)
				default:
					return nil
				}
			}
		}
	}
	return strarr
}

func convertOttoObjToIntArray(val otto.Value) ([]int64, bool) {
	var strarr []int64
	if val.IsNumber() {
		i, _ := val.ToInteger()
		strarr = append(strarr, i)
	} else {
		if val.Class() == "Array" {
			nativeVal, err := val.Export()
			if err != nil {
				glog.Errorln("Failed to export JS object.")
				return nil, false
			}
			for _, predicate := range nativeVal.([]interface{}) {
				switch v := predicate.(type) {
				case int64:
					strarr = append(strarr, v)
				default:
					return nil, false
				}
			}
		} else {
			return nil, false
		}
	}
	return strarr, true
}

func buildIteratorTree(obj *otto.Object, qs graph.QuadStore) graph.Iterator {
	if !isVertexChain(obj) {
		return iterator.NewNull()
	}
	return buildIteratorTreeHelper(obj, qs, iterator.NewNull())
}

func stringsFrom(obj *otto.Object) []string {
	var output []string
	lengthValue, _ := obj.Get("length")
	length, _ := lengthValue.ToInteger()
	ulength := uint32(length)
	for i := uint32(0); i < ulength; i++ {
		name := strconv.FormatInt(int64(i), 10)
		value, err := obj.Get(name)
		if err != nil || !value.IsString() {
			continue
		}
		output = append(output, value.String())
	}
	return output
}

func buildIteratorFromValue(val otto.Value, qs graph.QuadStore) graph.Iterator {
	if val.IsNull() || val.IsUndefined() {
		return qs.NodesAllIterator()
	}
	if val.IsPrimitive() {
		thing, _ := val.Export()
		switch v := thing.(type) {
		case string:
			it := qs.FixedIterator()
			it.Add(qs.ValueOf(v))
			return it
		default:
			glog.Errorln("Trying to build unknown primitive value.")
		}
	}
	switch val.Class() {
	case "Object":
		return buildIteratorTree(val.Object(), qs)
	case "Array":
		// Had better be an array of strings
		strings := stringsFrom(val.Object())
		it := qs.FixedIterator()
		for _, x := range strings {
			it.Add(qs.ValueOf(x))
		}
		return it
	case "Number":
		fallthrough
	case "Boolean":
		fallthrough
	case "Date":
		fallthrough
	case "String":
		it := qs.FixedIterator()
		it.Add(qs.ValueOf(val.String()))
		return it
	default:
		glog.Errorln("Trying to handle unsupported Javascript value.")
		return iterator.NewNull()
	}
}

func buildInOutIterator(obj *otto.Object, qs graph.QuadStore, base graph.Iterator, isReverse bool) graph.Iterator {
	argList, _ := obj.Get("_gremlin_values")
	if argList.Class() != "GoArray" {
		glog.Errorln("How is arglist not an array? Return nothing.", argList.Class())
		return iterator.NewNull()
	}
	argArray := argList.Object()
	lengthVal, _ := argArray.Get("length")
	length, _ := lengthVal.ToInteger()
	var predicateNodeIterator graph.Iterator
	if length == 0 {
		predicateNodeIterator = qs.NodesAllIterator()
	} else {
		zero, _ := argArray.Get("0")
		predicateNodeIterator = buildIteratorFromValue(zero, qs)
	}
	if length >= 2 {
		var tags []string
		one, _ := argArray.Get("1")
		if one.IsString() {
			tags = append(tags, one.String())
		} else if one.Class() == "Array" {
			tags = stringsFrom(one.Object())
		}
		for _, tag := range tags {
			predicateNodeIterator.Tagger().Add(tag)
		}
	}

	in, out := quad.Subject, quad.Object
	if isReverse {
		in, out = out, in
	}
	lto := iterator.NewLinksTo(qs, base, in)
	and := iterator.NewAnd(qs)
	and.AddSubIterator(iterator.NewLinksTo(qs, predicateNodeIterator, quad.Predicate))
	and.AddSubIterator(lto)
	return iterator.NewHasA(qs, and, out)
}

func buildInOutPredicateIterator(obj *otto.Object, qs graph.QuadStore, base graph.Iterator, isReverse bool) graph.Iterator {
	dir := quad.Subject
	if isReverse {
		dir = quad.Object
	}
	lto := iterator.NewLinksTo(qs, base, dir)
	hasa := iterator.NewHasA(qs, lto, quad.Predicate)
	return iterator.NewUnique(hasa)
}

func buildIteratorTreeHelper(obj *otto.Object, qs graph.QuadStore, base graph.Iterator) graph.Iterator {
	// TODO: Better error handling
	var (
		it    graph.Iterator
		subIt graph.Iterator
	)

	if prev, _ := obj.Get("_gremlin_prev"); !prev.IsObject() {
		subIt = base
	} else {
		subIt = buildIteratorTreeHelper(prev.Object(), qs, base)
	}

	stringArgs := propertiesOf(obj, "string_args")
	val, _ := obj.Get("_gremlin_type")
	switch val.String() {
	case "vertex":
		if len(stringArgs) == 0 {
			it = qs.NodesAllIterator()
		} else {
			fixed := qs.FixedIterator()
			for _, name := range stringArgs {
				fixed.Add(qs.ValueOf(name))
			}
			it = fixed
		}
	case "tag":
		it = subIt
		for _, tag := range stringArgs {
			it.Tagger().Add(tag)
		}
	case "save":
		all := qs.NodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return iterator.NewNull()
		}
		if len(stringArgs) == 2 {
			all.Tagger().Add(stringArgs[1])
		} else {
			all.Tagger().Add(stringArgs[0])
		}
		predFixed := qs.FixedIterator()
		predFixed.Add(qs.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd(qs)
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, all, quad.Object))
		hasa := iterator.NewHasA(qs, subAnd, quad.Subject)
		and := iterator.NewAnd(qs)
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "saver":
		all := qs.NodesAllIterator()
		if len(stringArgs) > 2 || len(stringArgs) == 0 {
			return iterator.NewNull()
		}
		if len(stringArgs) == 2 {
			all.Tagger().Add(stringArgs[1])
		} else {
			all.Tagger().Add(stringArgs[0])
		}
		predFixed := qs.FixedIterator()
		predFixed.Add(qs.ValueOf(stringArgs[0]))
		subAnd := iterator.NewAnd(qs)
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
		subAnd.AddSubIterator(iterator.NewLinksTo(qs, all, quad.Subject))
		hasa := iterator.NewHasA(qs, subAnd, quad.Object)
		and := iterator.NewAnd(qs)
		and.AddSubIterator(hasa)
		and.AddSubIterator(subIt)
		it = and
	case "has":
		args := propertyAsArray(obj, "_gremlin_values")
		argCount := len(args)

		if argCount < 2 || argCount > 3 {
			return iterator.NewNull() //TODO throw JS invalid args error
		}

		if argCount == 3 { // Has(<predicate>, <operator>, <comparison_value>)
			// iterator.Operator must be an integer
			if !args[1].IsNumber() {
				return iterator.NewNull() //TODO here be useuful error message
			}
			if !args[2].IsNumber() && !args[2].IsString() && args[2].Class() != "Array" {
				return iterator.NewNull()
			}
			predFixed := qs.FixedIterator()
			for _, name := range convertOttoObjToStrArray(args[0]) {
				predFixed.Add(qs.ValueOf(name))
			}
			var value graph.Value = args[2].String()
			var operator iterator.Operator

			val, _ := args[1].ToInteger()
			operator = iterator.Operator(val)

			if args[2].Class() == "Array" {
				if v, ok := convertOttoObjToIntArray(args[2]); ok {
					value = v
				} else {
					value = convertOttoObjToStrArray(args[2])
				}
			} else {
				if args[2].IsNumber() {
					val, _ := args[2].ToInteger()
					value = val
				}
				if args[2].IsString() {
					val, _ := args[2].ToString()
					value = val
				}
			}
			subAnd := iterator.NewAnd(qs)
			subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
			allObjs := iterator.NewLinksTo(qs, qs.NodesAllIterator(), quad.Object)
			subAnd.AddSubIterator(iterator.NewComparison(allObjs, operator, value, qs))
			hasa := iterator.NewHasA(qs, subAnd, quad.Subject)
			and := iterator.NewAnd(qs)
			and.AddSubIterator(hasa)
			and.AddSubIterator(subIt)
			it = and
		} else { // Has(<predicate>, <object>)
			predFixed := qs.FixedIterator()
			for _, name := range convertOttoObjToStrArray(args[0]) {
				predFixed.Add(qs.ValueOf(name))
			}
			objfixed := qs.FixedIterator()
			for _, name := range convertOttoObjToStrArray(args[1]) {
				objfixed.Add(qs.ValueOf(name))
			}
			subAnd := iterator.NewAnd(qs)
			subAnd.AddSubIterator(iterator.NewLinksTo(qs, predFixed, quad.Predicate))
			subAnd.AddSubIterator(iterator.NewLinksTo(qs, objfixed, quad.Object))
			hasa := iterator.NewHasA(qs, subAnd, quad.Subject)
			and := iterator.NewAnd(qs)
			and.AddSubIterator(hasa)
			and.AddSubIterator(subIt)
			it = and
		}
	case "morphism":
		it = base
	case "and":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		argIt := buildIteratorTree(firstArg.Object(), qs)

		and := iterator.NewAnd(qs)
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "back":
		arg, _ := obj.Get("_gremlin_back_chain")
		argIt := buildIteratorTree(arg.Object(), qs)
		and := iterator.NewAnd(qs)
		and.AddSubIterator(subIt)
		and.AddSubIterator(argIt)
		it = and
	case "is":
		fixed := qs.FixedIterator()
		for _, name := range stringArgs {
			fixed.Add(qs.ValueOf(name))
		}
		and := iterator.NewAnd(qs)
		and.AddSubIterator(fixed)
		and.AddSubIterator(subIt)
		it = and
	case "or":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		argIt := buildIteratorTree(firstArg.Object(), qs)

		or := iterator.NewOr()
		or.AddSubIterator(subIt)
		or.AddSubIterator(argIt)
		it = or
	case "both":
		// Hardly the most efficient pattern, but the most general.
		// Worth looking into an Optimize() optimization here.
		clone := subIt.Clone()
		it1 := buildInOutIterator(obj, qs, subIt, false)
		it2 := buildInOutIterator(obj, qs, clone, true)

		or := iterator.NewOr()
		or.AddSubIterator(it1)
		or.AddSubIterator(it2)
		it = or
	case "out":
		it = buildInOutIterator(obj, qs, subIt, false)
	case "follow":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}
		it = buildIteratorTreeHelper(firstArg.Object(), qs, subIt)
	case "followr":
		// Follow a morphism
		arg, _ := obj.Get("_gremlin_followr")
		if isVertexChain(arg.Object()) {
			return iterator.NewNull()
		}
		it = buildIteratorTreeHelper(arg.Object(), qs, subIt)
	case "in":
		it = buildInOutIterator(obj, qs, subIt, true)
	case "except":
		arg, _ := obj.Get("_gremlin_values")
		firstArg, _ := arg.Object().Get("0")
		if !isVertexChain(firstArg.Object()) {
			return iterator.NewNull()
		}

		allIt := qs.NodesAllIterator()
		toComplementIt := buildIteratorTree(firstArg.Object(), qs)
		notIt := iterator.NewNot(toComplementIt, allIt)

		and := iterator.NewAnd(qs)
		and.AddSubIterator(subIt)
		and.AddSubIterator(notIt)
		it = and
	case "in_predicates":
		it = buildInOutPredicateIterator(obj, qs, subIt, true)
	case "out_predicates":
		it = buildInOutPredicateIterator(obj, qs, subIt, false)
	}
	if it == nil {
		panic("Iterator building does not catch the output iterator in some case.")
	}
	return it
}
