package cfgstruct

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

type TestStruct struct {
	String              string        `default:"dev"`
	StringSlice         []string      `default:"dev"`
	StringSliceMultiple []string      `default:"dev,test"`
	StringSliceEmpty    []string      `default:""`
	Bool                bool          `releaseDefault:"false" devDefault:"true"`
	Int64               int64         `releaseDefault:"0" devDefault:"1" testDefault:"2"`
	Int                 int           `default:"2"`
	Uint64              uint64        `default:"3" releaseDefault:"2"`
	Uint                uint          `default:"0" devDefault:"2"`
	Float64             float64       `default:"5.5" testDefault:"1"`
	Duration            time.Duration `default:"1h" testDefault:"$TESTINTERVAL"`
	Struct              struct {
		AnotherString string `default:"dev2"`
	}
	Fields [10]struct {
		AnotherInt int `default:"6"`
	}

	// Map won't be used as CLI parameter, but might be used from config file.
	Map map[string]struct {
		Name string
	} `noflag:"true"`
}

func TestBind(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	var c TestStruct
	Bind(f, &c, UseReleaseDefaults())

	require.Equal(t, c.String, string("dev"))
	require.Equal(t, c.StringSlice, []string{"dev"})
	require.Equal(t, c.StringSliceMultiple, []string{"dev", "test"})
	require.Equal(t, c.StringSliceEmpty, []string{})
	require.Equal(t, c.Bool, bool(false))
	require.Equal(t, c.Int64, int64(0))
	require.Equal(t, c.Int, int(2))
	require.Equal(t, c.Uint64, uint64(2))
	require.Equal(t, c.Uint, uint(0))
	require.Equal(t, c.Float64, float64(5.5))
	require.Equal(t, c.Duration, time.Hour)
	require.Equal(t, c.Struct.AnotherString, string("dev2"))
	require.Equal(t, c.Fields[0].AnotherInt, int(6))
	require.Equal(t, c.Fields[3].AnotherInt, int(6))

	err := f.Parse([]string{
		"--string=1",
		"--string-slice-multiple=alpha,beta",
		"--string-slice=1",
		"--string-slice=2,3",
		"--bool=true",
		"--int64=1",
		"--int=1",
		"--uint64=1",
		"--uint=1",
		"--float64=1",
		"--size=1MiB",
		"--duration=1h",
		"--node-url=12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S@mars.tardigrade.io:7777",
		"--node-ur-ls=12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S@mars.tardigrade.io:7777,12L9ZFwhzVpuEKMUNUqkaTLGzwY9G24tbiigLiXpmZWKwmcNDDs@jupiter.tardigrade.io:7777",
		"--struct.another-string=1",
		"--fields.03.another-int=1"})
	if err != nil {
		panic(err)
	}
	require.Equal(t, c.String, string("1"))
	require.Equal(t, c.StringSliceMultiple, []string{"alpha", "beta"})
	require.Equal(t, c.StringSlice, []string{"1", "2", "3"})
	require.Equal(t, c.StringSliceEmpty, []string{})
	require.Equal(t, c.Bool, bool(true))
	require.Equal(t, c.Int64, int64(1))
	require.Equal(t, c.Int, int(1))
	require.Equal(t, c.Uint64, uint64(1))
	require.Equal(t, c.Uint, uint(1))
	require.Equal(t, c.Float64, float64(1))
	require.Equal(t, c.Duration, time.Hour)
	require.Equal(t, c.Struct.AnotherString, string("1"))
	require.Equal(t, c.Fields[0].AnotherInt, int(6))
	require.Equal(t, c.Fields[3].AnotherInt, int(1))
}

func TestConfDir(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	var c struct {
		String    string `default:"-$CONFDIR+"`
		MyStruct1 struct {
			String    string `default:"1${CONFDIR}2"`
			MyStruct2 struct {
				String string `default:"2${CONFDIR}3"`
			}
		}
	}
	Bind(f, &c, UseReleaseDefaults(), ConfDir("confpath"))
	require.Equal(t, f.Lookup("string").DefValue, "-confpath+")
	require.Equal(t, f.Lookup("my-struct1.string").DefValue, "1confpath2")
	require.Equal(t, f.Lookup("my-struct1.my-struct2.string").DefValue, "2confpath3")
}

func TestBindDevDefaults(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	var c TestStruct
	Bind(f, &c, UseDevDefaults())

	require.Equal(t, c.String, string("dev"))
	require.Equal(t, c.Bool, bool(true))
	require.Equal(t, c.Int64, int64(1))
	require.Equal(t, c.Int, int(2))
	require.Equal(t, c.Uint64, uint64(3))
	require.Equal(t, c.Uint, uint(2))
	require.Equal(t, c.Float64, float64(5.5))
	require.Equal(t, c.Duration, time.Hour)
	require.Equal(t, c.Struct.AnotherString, string("dev2"))
	require.Equal(t, c.Fields[0].AnotherInt, int(6))
	require.Equal(t, c.Fields[3].AnotherInt, int(6))

	err := f.Parse([]string{
		"--string=1",
		"--bool=true",
		"--int64=1",
		"--int=1",
		"--uint64=1",
		"--uint=1",
		"--float64=1",
		"--duration=1h",
		"--node-url=121RTSDpyNZVcEU84Ticf2L1ntiuUimbWgfATz21tuvgk3vzoA6@saturn.tardigrade.io:7777",
		"--node-ur-ls=121RTSDpyNZVcEU84Ticf2L1ntiuUimbWgfATz21tuvgk3vzoA6@saturn.tardigrade.io:7777",
		"--struct.another-string=1",
		"--fields.03.another-int=1"})
	if err != nil {
		panic(err)
	}
	require.Equal(t, c.String, string("1"))
	require.Equal(t, c.Bool, bool(true))
	require.Equal(t, c.Int64, int64(1))
	require.Equal(t, c.Int, int(1))
	require.Equal(t, c.Uint64, uint64(1))
	require.Equal(t, c.Uint, uint(1))
	require.Equal(t, c.Float64, float64(1))
	require.Equal(t, c.Duration, time.Hour)
	require.Equal(t, c.Struct.AnotherString, string("1"))
	require.Equal(t, c.Fields[0].AnotherInt, int(6))
	require.Equal(t, c.Fields[3].AnotherInt, int(1))
}

func TestHiddenDev(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	var c struct {
		String  string `default:"dev" hidden:"true"`
		String2 string `default:"dev" hidden:"false"`
		Bool    bool   `releaseDefault:"false" devDefault:"true" hidden:"true"`
		Int64   int64  `releaseDefault:"0" devDefault:"1"`
		Int     int    `default:"2"`
	}
	Bind(f, &c, UseDevDefaults())

	flagString := f.Lookup("string")
	flagStringHide := f.Lookup("string2")
	flagBool := f.Lookup("bool")
	flagInt64 := f.Lookup("int64")
	flagInt := f.Lookup("int")
	flagSize := f.Lookup("size")
	require.Equal(t, flagString.Hidden, true)
	require.Equal(t, flagStringHide.Hidden, false)
	require.Equal(t, flagBool.Hidden, true)
	require.Equal(t, flagInt64.Hidden, false)
	require.Equal(t, flagInt.Hidden, false)
	require.Equal(t, flagSize.Hidden, true)
}

func TestHiddenRelease(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	var c struct {
		String  string `default:"dev" hidden:"false"`
		String2 string `default:"dev" hidden:"true"`
		Bool    bool   `releaseDefault:"false" devDefault:"true" hidden:"true"`
		Int64   int64  `releaseDefault:"0" devDefault:"1"`
		Int     int    `default:"2"`
	}
	Bind(f, &c, UseReleaseDefaults())

	flagString := f.Lookup("string")
	flagStringHide := f.Lookup("string2")
	flagBool := f.Lookup("bool")
	flagInt64 := f.Lookup("int64")
	flagInt := f.Lookup("int")
	require.Equal(t, flagString.Hidden, false)
	require.Equal(t, flagStringHide.Hidden, true)
	require.Equal(t, flagBool.Hidden, true)
	require.Equal(t, flagInt64.Hidden, false)
	require.Equal(t, flagInt.Hidden, false)
}

func TestSource(t *testing.T) {
	var c struct {
		Unset string
		Any   string `source:"any"`
		Flag  string `source:"flag"`
	}

	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	Bind(f, &c, UseReleaseDefaults())

	unset := f.Lookup("unset")
	require.NotNil(t, unset)
	require.Empty(t, unset.Annotations)

	any := f.Lookup("any")
	require.NotNil(t, any)
	require.Equal(t, map[string][]string{
		"source": {"any"},
	}, any.Annotations)

	flag := f.Lookup("flag")
	require.NotNil(t, flag)
	require.Equal(t, map[string][]string{
		"source": {"flag"},
	}, flag.Annotations)
}

func TestBindTestDefaults(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)
	var c TestStruct
	Bind(f, &c, UseTestDefaults(), ConfigVar("TESTINTERVAL", "30s"))

	require.Equal(t, c.String, string("dev"))
	require.Equal(t, c.Bool, bool(true))
	require.Equal(t, c.Int64, int64(2))
	require.Equal(t, c.Int, int(2))
	require.Equal(t, c.Uint64, uint64(3))
	require.Equal(t, c.Uint, uint(2))
	require.Equal(t, c.Float64, float64(1))
	require.Equal(t, c.Duration, 30*time.Second)
	require.Equal(t, c.Struct.AnotherString, string("dev2"))
	require.Equal(t, c.Fields[0].AnotherInt, int(6))
	require.Equal(t, c.Fields[3].AnotherInt, int(6))

	err := f.Parse([]string{
		"--string=1",
		"--bool=true",
		"--int64=1",
		"--int=1",
		"--uint64=1",
		"--uint=1",
		"--float64=1",
		"--duration=1h",
		"--node-url=121RTSDpyNZVcEU84Ticf2L1ntiuUimbWgfATz21tuvgk3vzoA6@saturn.tardigrade.io:7777",
		"--node-ur-ls=121RTSDpyNZVcEU84Ticf2L1ntiuUimbWgfATz21tuvgk3vzoA6@saturn.tardigrade.io:7777",
		"--struct.another-string=1",
		"--fields.03.another-int=1"})
	if err != nil {
		panic(err)
	}
	require.Equal(t, c.String, string("1"))
	require.Equal(t, c.Bool, bool(true))
	require.Equal(t, c.Int64, int64(1))
	require.Equal(t, c.Int, int(1))
	require.Equal(t, c.Uint64, uint64(1))
	require.Equal(t, c.Uint, uint(1))
	require.Equal(t, c.Float64, float64(1))
	require.Equal(t, c.Duration, time.Hour)
	require.Equal(t, c.Struct.AnotherString, string("1"))
	require.Equal(t, c.Fields[0].AnotherInt, int(6))
	require.Equal(t, c.Fields[3].AnotherInt, int(1))
}

func TestWrongSyntax(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.ContinueOnError)
	var c TestStruct
	Bind(f, &c, UseReleaseDefaults())
	err := f.Parse([]string{
		"--node-url=foo@bar",
	})
	require.Error(t, err)
}

func TestBindWithPrefix(t *testing.T) {
	f := pflag.NewFlagSet("test", pflag.PanicOnError)

	var c struct {
		Flag string
	}
	Bind(f, &c, UseTestDefaults(), Prefix("pfx"))

	err := f.Parse([]string{
		"--pfx.flag=2",
	})

	require.NoError(t, err)
	require.Equal(t, c.Flag, string("2"))

}
