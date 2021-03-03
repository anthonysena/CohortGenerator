# Run tests
devtools::test()

# Ref: https://jozef.io/r104-unit-testing-coverage/
# Capture test coverage - run in the package root
covResults <- covr::package_coverage()
# Visually inspect the code to see where we lack coverage
zeroCov <- covr::zero_coverage(covResults)