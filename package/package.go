package cmmio

import (
	ccore "github.com/jurgen-kluft/ccore/package"
	cunittest "github.com/jurgen-kluft/cunittest/package"
	"github.com/jurgen-kluft/go-ide/denv"
)

const (
	repo_path = "github.com\\jurgen-kluft"
	repo_name = "cmmio"
)

func GetPackage() *denv.Package {
	name := repo_name

	// dependencies
	cunittestpkg := cunittest.GetPackage()
	ccorepkg := ccore.GetPackage()

	// main package
	mainpkg := denv.NewPackage(repo_path, repo_name)
	mainpkg.AddPackage(cunittestpkg)
	mainpkg.AddPackage(ccorepkg)

	// main library
	mainlib := denv.SetupCppLibProject(mainpkg, name)
	mainlib.AddDependencies(ccorepkg.GetMainLib())

	// test library
	testlib := denv.SetupCppTestLibProject(mainpkg, name)
	testlib.AddDependencies(ccorepkg.GetTestLib())

	// unittest project
	maintest := denv.SetupCppTestProject(mainpkg, name)
	maintest.CopyToOutput("source/test/data", "*.bin", "data")
	maintest.AddDependencies(cunittestpkg.GetTestLib())
	maintest.AddDependency(testlib)

	// producer application
	producerApp := denv.SetupCppAppProject(mainpkg, "producer", "producer")
	producerApp.AddDependency(mainlib)

	mainpkg.AddMainApp(producerApp)
	mainpkg.AddMainLib(mainlib)
	mainpkg.AddTestLib(testlib)
	mainpkg.AddUnittest(maintest)
	return mainpkg
}
