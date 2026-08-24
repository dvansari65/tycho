// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.26;

import "../TychoRouterTestSetup.sol";
import "@src/executors/NativeExecutor.sol";
import {TransferManager} from "@src/TransferManager.sol";
import {Constants} from "../Constants.sol";
import "forge-std/Test.sol";

// Mocks

contract MockNativeRouter {
    bool public called;
    uint256 public lastValue;
    bytes public lastCalldata;
    address public lastCaller;

    receive() external payable {}

    fallback() external payable {
        called = true;
        lastValue = msg.value;
        lastCalldata = msg.data;
        lastCaller = msg.sender;
    }
}

// Unit Tests

contract NativeExecutorUnitTest is Test, Constants {
    NativeExecutor executor;
    MockNativeRouter mockV4;

    address constant BAD_TARGET = address(0xdead);
    uint32 constant AMOUNT_IN_OFFSET = 36;

    function setUp() public {
        mockV4 = new MockNativeRouter();
        executor = new NativeExecutor(address(mockV4));
    }

    // Constructor tests

    function test_Constructor_StoresAddresses() public view {
        assertEq(executor.nativeRouterV4(), address(mockV4));
    }

    function test_Constructor_Reverts_ZeroAddress() public {
        vm.expectRevert(NativeExecutor__ZeroAddress.selector);
        new NativeExecutor(address(0));
    }

    function test_Constructor_Reverts_NotAContract() public {
        address eoa = makeAddr("eoa");

        vm.expectRevert(NativeExecutor__NotAContract.selector);
        new NativeExecutor(eoa);
    }

    // fundsExpectedAddress tests

    function test_FundsExpectedAddress_ReturnsMsgSender() public view {
        assertEq(executor.fundsExpectedAddress(hex""), address(this));
    }

    // getTransferData tests

    function test_GetTransferData_ERC20() public view {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, address(mockV4), payload);

        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        ) = executor.getTransferData(data);

        assertEq(
            uint8(transferType),
            uint8(TransferManager.TransferType.ProtocolWillDebit)
        );
        assertEq(receiver, address(mockV4));
        assertEq(tokenIn, USDC_ADDR);
        assertEq(tokenOut, ETH_ADDR);
        assertTrue(outputToRouter);
    }

    function test_GetTransferData_NativeETH() public view {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(ETH_ADDR, USDC_ADDR, address(mockV4), payload);

        (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        ) = executor.getTransferData(data);

        assertEq(
            uint8(transferType),
            uint8(TransferManager.TransferType.TransferNativeInExecutor)
        );
        assertEq(receiver, address(0));
        assertEq(tokenIn, ETH_ADDR);
        assertEq(tokenOut, USDC_ADDR);
        assertTrue(outputToRouter);
    }

    function test_GetTransferData_Reverts_InvalidTarget() public {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, BAD_TARGET, payload);

        vm.expectRevert(NativeExecutor__InvalidTarget.selector);
        executor.getTransferData(data);
    }

    function test_GetTransferData_Reverts_TruncatedPayload() public {
        bytes memory payload =
            abi.encodePacked(executor.TRADE_RFQT_SELECTOR(), new bytes(95));
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, address(mockV4), payload);

        vm.expectRevert(NativeExecutor__InvalidDataLength.selector);
        executor.getTransferData(data);
    }

    // swap tests

    function test_Swap_ERC20_SendsZeroEth() public {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, address(mockV4), payload);

        executor.swap(1000000, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 0);
        assertEq(mockV4.lastCaller(), address(executor));
        assertEq(_wordAt(mockV4.lastCalldata(), AMOUNT_IN_OFFSET), 0);
    }

    function test_Swap_ETH_ForwardsAmountIn() public {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(ETH_ADDR, USDC_ADDR, address(mockV4), payload);

        vm.deal(address(this), 1 ether);
        executor.swap{value: 1 ether}(1 ether, data, address(0));

        assertTrue(mockV4.called());
        assertEq(mockV4.lastValue(), 1 ether);
        assertEq(_wordAt(mockV4.lastCalldata(), AMOUNT_IN_OFFSET), 0);
    }

    function test_Swap_ETH_Reverts_ZeroAmountIn() public {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(ETH_ADDR, USDC_ADDR, address(mockV4), payload);

        vm.expectRevert(NativeExecutor__InvalidAmountIn.selector);
        executor.swap(0, data, address(0));
    }

    function test_Swap_Reverts_InvalidTarget() public {
        bytes memory payload = _tradePayload();
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, BAD_TARGET, payload);

        vm.expectRevert(NativeExecutor__InvalidTarget.selector);
        executor.swap(1000000, data, address(0));
    }

    function test_Swap_Reverts_InvalidSelector() public {
        bytes4 badSelector = 0x12345678;
        bytes memory payload = abi.encodeWithSelector(badSelector, hex"1234");
        bytes memory data =
            _encodeExecutorData(USDC_ADDR, ETH_ADDR, address(mockV4), payload);

        vm.expectRevert(NativeExecutor__InvalidPayload.selector);
        executor.swap(1000000, data, address(0));
    }

    function test_Swap_Reverts_ShortData() public {
        vm.expectRevert(NativeExecutor__InvalidDataLength.selector);
        executor.swap(1000000, hex"1234", address(0));
    }

    function test_Swap_ERC20_OverridesAmountOnUnderDelivery() public {
        uint256 signedAmountIn = 1_000_000;
        uint256 actualAmountIn = signedAmountIn - 1;
        bytes memory data = _encodeExecutorData(
            USDC_ADDR,
            ETH_ADDR,
            address(mockV4),
            signedAmountIn,
            AMOUNT_IN_OFFSET,
            _tradePayload()
        );

        executor.swap(actualAmountIn, data, address(0));

        assertTrue(mockV4.called());
        assertEq(
            _wordAt(mockV4.lastCalldata(), AMOUNT_IN_OFFSET), actualAmountIn
        );
        assertEq(mockV4.lastValue(), 0);
    }

    function test_Swap_ETH_OverridesAmountAndForwardsActualValue() public {
        uint256 signedAmountIn = 1 ether;
        uint256 actualAmountIn = signedAmountIn - 1;
        bytes memory data = _encodeExecutorData(
            ETH_ADDR,
            USDC_ADDR,
            address(mockV4),
            signedAmountIn,
            AMOUNT_IN_OFFSET,
            _tradePayload()
        );

        vm.deal(address(this), actualAmountIn);
        executor.swap{value: actualAmountIn}(actualAmountIn, data, address(0));

        assertEq(
            _wordAt(mockV4.lastCalldata(), AMOUNT_IN_OFFSET), actualAmountIn
        );
        assertEq(mockV4.lastValue(), actualAmountIn);
    }

    function test_Swap_Reverts_WhenAmountExceedsSignedAmount() public {
        bytes memory data = _encodeExecutorData(
            USDC_ADDR,
            ETH_ADDR,
            address(mockV4),
            1_000_000,
            AMOUNT_IN_OFFSET,
            _tradePayload()
        );

        vm.expectRevert(NativeExecutor__InvalidAmountIn.selector);
        executor.swap(1_000_001, data, address(0));
    }

    function test_Swap_Reverts_MisalignedAmountInOffset() public {
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV4), 1_000_000, 37, _tradePayload()
        );

        vm.expectRevert(NativeExecutor__InvalidAmountInOffset.selector);
        executor.swap(1_000_000, data, address(0));
    }

    function test_Swap_Reverts_AmountInOffsetInsideSelector() public {
        bytes memory data = _encodeExecutorData(
            USDC_ADDR, ETH_ADDR, address(mockV4), 1_000_000, 0, _tradePayload()
        );

        vm.expectRevert(NativeExecutor__InvalidAmountInOffset.selector);
        executor.swap(1_000_000, data, address(0));
    }

    function test_Swap_Reverts_OutOfBoundsAmountInOffset() public {
        bytes memory data = _encodeExecutorData(
            USDC_ADDR,
            ETH_ADDR,
            address(mockV4),
            1_000_000,
            100,
            _tradePayload()
        );

        vm.expectRevert(NativeExecutor__InvalidAmountInOffset.selector);
        executor.swap(1_000_000, data, address(0));
    }

    // Helper

    function _tradePayload() internal view returns (bytes memory) {
        return abi.encodePacked(
            executor.TRADE_RFQT_SELECTOR(),
            bytes32(uint256(0x60)),
            bytes32(uint256(0)),
            bytes32(uint256(0))
        );
    }

    function _wordAt(bytes memory data, uint256 offset)
        internal
        pure
        returns (uint256 value)
    {
        assembly ("memory-safe") {
            value := mload(add(add(data, 0x20), offset))
        }
    }

    function _encodeExecutorData(
        address tokenIn,
        address tokenOut,
        address target,
        bytes memory payload
    ) internal view returns (bytes memory) {
        uint256 signedAmountIn = tokenIn == ETH_ADDR ? 1 ether : 1_000_000;
        return _encodeExecutorData(
            tokenIn, tokenOut, target, signedAmountIn, AMOUNT_IN_OFFSET, payload
        );
    }

    function _encodeExecutorData(
        address tokenIn,
        address tokenOut,
        address target,
        uint256 signedAmountIn,
        uint32 amountInOffset,
        bytes memory payload
    ) internal pure returns (bytes memory) {
        return abi.encodePacked(
            bytes20(tokenIn),
            bytes20(tokenOut),
            bytes20(target),
            bytes4(amountInOffset),
            bytes32(signedAmountIn),
            payload
        );
    }
}

// Integration Tests

contract NativeExecutorForkTest is Test, Constants {
    uint256 private constant FORK_BLOCK = 25747646;

    NativeExecutor nativeExecutor;

    function setUp() public {
        vm.createSelectFork(vm.rpcUrl("mainnet"), FORK_BLOCK);
        nativeExecutor = new NativeExecutor(NATIVE_ROUTER_V4);
    }

    function test_RecordedQuoteUnderDeliveryAgainstRealNativeRouter() public {
        IERC20 USDC = IERC20(USDC_ADDR);
        IERC20 WETH = IERC20(WETH_ADDR);
        uint256 signedAmountIn = 1_000_000;
        uint256 actualAmountIn = signedAmountIn - 1;
        uint256 balanceBefore = WETH.balanceOf(ALICE);

        // Recorded from Native's firm-quote API for signedAmountIn and this recipient.
        // We execute with one wei less to exercise actualSellerAmount. Pinning the fork
        // and timestamp keeps the signed quote deterministic and CI independent from the API.
        bytes memory payload =
            hex"0947c2d9000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000c419e67388df0c0cfad15584fc5fc7e67a234c17000000000000000000000000129b3d9a0a6e4beab88f5cb1e57995d72a6e24f1000000000000000000000000cd09f75e2bf2a4d11f3ab23f1389fcc1621c0cc2000000000000000000000000a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48000000000000000000000000c02aaa39b223fe8d0a0e5c4f27ead9083c756cc200000000000000000000000000000000000000000000000000000000000f42400000000000000000000000000000000000000000000000000001e485be8291920000000000000000000000000000000000000000000000000001e485be829192000000000000000000000000000000000000000000000000000000006a7e01780000000000000000000000000000000000000000000000001515091293fe9336000000000000000000000000000000000000000000000000000000006a7e012f000000000000000000000000000000000000000000000000000000006a7e0157000000000000000000000000000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000003c6ceafc34ce4a73ab5b91ee96b7cdb000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000002800000000000000000000000006044eef7179034319e2c8636ea885b37cbfa9aba00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000300000000000000000000000000000000000000000000000000000000000000004109dcf6acddf595c5a2914635e3e1b92b4c258b9152e735170680e8630b1caa50063f0ce8e6671436c04ce3b2cf28a843a41058bda799e76ca76477f694d261371b000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000041f0eaef9eed6596ed3964c1d57686c75f8e1a98e1d406d366c477d63948cabcc940afad951e0d03942e12e1ca0c94692a13d32641c8e75f125db6cc7be456f0d31b00000000000000000000000000000000000000000000000000000000000000";
        bytes memory data = abi.encodePacked(
            bytes20(USDC_ADDR),
            bytes20(WETH_ADDR),
            bytes20(NATIVE_ROUTER_V4),
            bytes4(uint32(36)),
            bytes32(signedAmountIn),
            payload
        );

        deal(USDC_ADDR, address(nativeExecutor), actualAmountIn);
        vm.prank(address(nativeExecutor));
        USDC.approve(NATIVE_ROUTER_V4, actualAmountIn);
        vm.warp(1_786_642_807);

        nativeExecutor.swap(actualAmountIn, data, ALICE);

        assertGt(WETH.balanceOf(ALICE) - balanceBefore, 0);
        assertEq(USDC.balanceOf(address(nativeExecutor)), 0);
    }
}

contract TychoRouterNativeIntegrationTest is TychoRouterTestSetup {
    function getForkBlock() public pure override returns (uint256) {
        // The two firm quotes below were recorded against this block for the
        // deterministic Tycho Router address deployed by TychoRouterTestSetup.
        return 25816459;
    }

    function test_RecordedQuoteERC20InputThroughTychoRouter() public {
        IERC20 USDC = IERC20(USDC_ADDR);
        uint256 amountIn = 3000000000;
        uint256 amountOut = 1_255_650_775_965_669_400;

        deal(address(USDC), ALICE, amountIn);
        uint256 balanceBefore = BOB.balance;

        vm.startPrank(ALICE);
        USDC.approve(tychoRouterAddr, type(uint256).max);

        bytes memory callData =
            loadCallDataFromFile("test_single_encoding_strategy_native");

        (bool success,) = tychoRouterAddr.call(callData);
        vm.stopPrank();

        uint256 balanceAfter = BOB.balance;
        assertTrue(success, "Call Failed");
        assertEq(balanceAfter - balanceBefore, amountOut);
        assertEq(USDC.balanceOf(tychoRouterAddr), 0);
        assertEq(tychoRouterAddr.balance, 0);
    }

    function test_RecordedQuoteNativeInputThroughTychoRouter() public {
        IERC20 USDC = IERC20(USDC_ADDR);
        uint256 amountIn = 1 ether;
        uint256 amountOut = 2_388_254_994;

        deal(ALICE, amountIn);

        bytes memory callData = loadCallDataFromFile(
            "test_single_encoding_strategy_native_eth_input"
        );
        uint256 balanceBefore = USDC.balanceOf(BOB);

        vm.prank(ALICE);
        (bool success,) = tychoRouterAddr.call{value: amountIn}(callData);

        assertTrue(success, "Call Failed");
        assertEq(USDC.balanceOf(BOB) - balanceBefore, amountOut);
        assertEq(USDC.balanceOf(tychoRouterAddr), 0);
        assertEq(tychoRouterAddr.balance, 0);
    }
}
