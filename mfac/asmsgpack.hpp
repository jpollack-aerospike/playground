// Copyright 2013-2023 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#pragma once

#include <string>
#include <vector>
#include <memory>
#include <type_traits> // std::enable_if
#include <istream> // std::basic_istream
#include <jsoncons/json.hpp>
#include <jsoncons/config/jsoncons_config.hpp>
#include "asmsgpack/msgpack_encoder.hpp"
#include "asmsgpack/msgpack_reader.hpp"
#include "asmsgpack/msgpack_cursor.hpp"
#include "asmsgpack/encode_msgpack.hpp"
#include "asmsgpack/decode_msgpack.hpp"

