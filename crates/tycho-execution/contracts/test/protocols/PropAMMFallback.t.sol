pragma solidity ^0.8.26;

import "../TychoRouterTestSetup.sol";
import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";
import {
    PropAMMFallbackExecutor,
    PropAMMFallbackExecutor__InvalidDataLength
} from "../../src/executors/PropAMMFallbackExecutor.sol";
import {IPropAMMRouter} from "@interfaces/IPropAMMRouter.sol";
import {IPropAMM} from "@interfaces/IPropAMM.sol";
import {TransferManager} from "../../src/TransferManager.sol";

contract PropAMMFallbackExecutorExposed is PropAMMFallbackExecutor {
    function decodeParams(bytes calldata data)
        external
        pure
        returns (address venue, address tokenIn, address tokenOut)
    {
        return _decodeData(data);
    }
}

contract PropAMMFallbackExecutorTest is TestUtils, Constants {
    PropAMMFallbackExecutorExposed executor;

    /// Block at which FermiSwap's oracle lane is stale, so the venue reverts and the
    /// PropAMMRouter's Uniswap V3 retry is the path under test. This is the ordinary case for any
    /// block Titan did not build: it is what makes integrator simulations of pAMM routes fail.
    function setUp() public {
        vm.createSelectFork(vm.rpcUrl("mainnet"), 25682938);
        executor = new PropAMMFallbackExecutorExposed();
    }

    /// The router is hardcoded, so a deployment can never point somewhere else.
    function testRouterAddress() public view {
        assertEq(address(executor.PROPAMM_ROUTER()), PROPAMM_ROUTER);
    }

    function testDecodeParams() public view {
        (address venue, address tokenIn, address tokenOut) = executor.decodeParams(
            abi.encodePacked(FERMI_PROPAMM_VENUE, WETH_ADDR, USDC_ADDR)
        );

        assertEq(venue, FERMI_PROPAMM_VENUE);
        assertEq(tokenIn, WETH_ADDR);
        assertEq(tokenOut, USDC_ADDR);
    }

    function testDecodeParamsInvalidDataLength() public {
        vm.expectRevert(PropAMMFallbackExecutor__InvalidDataLength.selector);
        executor.decodeParams(abi.encodePacked(FERMI_PROPAMM_VENUE, WETH_ADDR));
    }

    function testGetTransferData() public view {
        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        ) = executor.getTransferData(
            abi.encodePacked(FERMI_PROPAMM_VENUE, WETH_ADDR, USDC_ADDR)
        );

        // The PropAMMRouter pulls tokenIn with transferFrom, unlike the push-payment
        // PropAMMExecutor.
        assertEq(
            uint8(transferType),
            uint8(TransferManager.TransferType.ProtocolWillDebit)
        );
        assertEq(receiver, PROPAMM_ROUTER);
        assertEq(tokenIn, WETH_ADDR);
        assertEq(tokenOut, USDC_ADDR);
        assertFalse(outputToRouter);
    }

    /// The venue is inactive at this block, so a direct call would revert.
    function testVenueQuoteAtForkBlock() public {
        vm.expectRevert();
        IPropAMM(FERMI_PROPAMM_VENUE).quote(WETH_ADDR, USDC_ADDR, 1 ether);
    }

    /// The whole point: the leg still delivers tokenOut, at the Uniswap V3 price.
    function testSwapWithStaleVenue() public {
        uint256 amountIn = 1 ether;

        deal(WETH_ADDR, address(executor), amountIn);
        vm.prank(address(executor));
        IERC20(WETH_ADDR).approve(PROPAMM_ROUTER, amountIn);

        uint256 usdcBefore = IERC20(USDC_ADDR).balanceOf(BOB);
        executor.swap(
            amountIn,
            abi.encodePacked(FERMI_PROPAMM_VENUE, WETH_ADDR, USDC_ADDR),
            BOB
        );
        uint256 usdcDelta = IERC20(USDC_ADDR).balanceOf(BOB) - usdcBefore;

        // Uniswap V3 WETH/USDC at the router's resolvedFee tier, ~1872 USDC at this block.
        assertGt(usdcDelta, 1800e6);
        assertEq(IERC20(WETH_ADDR).balanceOf(address(executor)), 0);
    }

    /// The fallback pool is chosen by the router, not by us. A pair with no pool at
    /// `resolvedFee` has no fallback, so Fynd must not route through here for it.
    function testResolvedFee() public view {
        assertEq(
            IPropAMMRouter(PROPAMM_ROUTER).resolvedFee(WETH_ADDR, USDC_ADDR),
            500
        );
    }
}
