// SPDX-License-Identifier: BUSL-1.1
pragma solidity ^0.8.26;

import {TransferManager} from "../TransferManager.sol";
import {IExecutor} from "@interfaces/IExecutor.sol";
import {Address} from "@openzeppelin/contracts/utils/Address.sol";
import {ETH_ADDRESS} from "../../lib/NativeETH.sol";

error NativeExecutor__InvalidDataLength();
error NativeExecutor__InvalidTarget();
error NativeExecutor__InvalidPayload();
error NativeExecutor__InvalidAmountIn();
error NativeExecutor__InvalidAmountInOffset();
error NativeExecutor__ZeroAddress();
error NativeExecutor__NotAContract();

contract NativeExecutor is IExecutor {
    using Address for address;

    address public immutable nativeRouterV4;

    // Native Router entrypoint:
    // tradeRFQT(RFQTQuote quote, uint256 actualSellerAmount, uint256 actualMinOutputAmount)
    bytes4 public constant TRADE_RFQT_SELECTOR = 0x0947c2d9;
    uint256 private constant _FIXED_HEADER_LENGTH = 96;
    uint256 private constant _MIN_TRADE_RFQT_CALLDATA_LENGTH = 4 + 3 * 32;

    constructor(address _nativeRouterV4) {
        if (_nativeRouterV4 == address(0)) {
            revert NativeExecutor__ZeroAddress();
        }
        if (_nativeRouterV4.code.length == 0) {
            revert NativeExecutor__NotAContract();
        }
        nativeRouterV4 = _nativeRouterV4;
    }

    function fundsExpectedAddress(
        bytes calldata /* data */
    )
        external
        view
        returns (address receiver)
    {
        return msg.sender;
    }

    function swap(uint256 amountIn, bytes calldata data, address receiver)
        external
        payable
    {
        (
            address tokenIn,
            /* address tokenOut */,
            address target,
            uint32 amountInOffset,
            uint256 signedAmountIn,
            bytes memory payload
        ) = _decodeData(data);

        if (!_isValidTarget(target)) {
            revert NativeExecutor__InvalidTarget();
        }

        // check payload against function selector
        bytes4 selector = bytes4(payload);
        if (selector != TRADE_RFQT_SELECTOR) {
            revert NativeExecutor__InvalidPayload();
        }

        // Native treats a zero actualSellerAmount as "use the signed amount".
        // Therefore zero cannot represent an actual zero input.
        if (amountIn == 0 || signedAmountIn == 0) {
            revert NativeExecutor__InvalidAmountIn();
        }

        _validateAmountInOffset(payload, amountInOffset);

        if (amountIn != signedAmountIn) {
            _setActualSellerAmount(payload, amountInOffset, amountIn);
        }

        // amountIn is authoritative at execution time. It equals Native's quoted
        // payable value for exact fills and reflects the amount delivered by the
        // preceding hop for composed swaps.
        uint256 executionValue = tokenIn == ETH_ADDRESS ? amountIn : 0;

        // slither-disable-next-line unused-return
        target.functionCallWithValue(payload, executionValue);
    }

    function _decodeData(bytes calldata data)
        internal
        pure
        returns (
            address tokenIn,
            address tokenOut,
            address target,
            uint32 amountInOffset,
            uint256 signedAmountIn,
            bytes memory payload
        )
    {
        // Decode the 96-byte fixed header injected by NativeSwapEncoder.
        // 20 tokenIn + 20 tokenOut + 20 target + 4 amountInOffset
        // + 32 signedAmountIn = 96 bytes.
        // The tradeRFQT payload must contain its 4-byte selector and three
        // 32-byte ABI head words. Its dynamic quote data remains opaque and is
        // validated by the Native Router.
        if (
            data.length < _FIXED_HEADER_LENGTH + _MIN_TRADE_RFQT_CALLDATA_LENGTH
        ) {
            revert NativeExecutor__InvalidDataLength();
        }

        tokenIn = address(bytes20(data[0:20]));
        tokenOut = address(bytes20(data[20:40]));
        target = address(bytes20(data[40:60]));
        amountInOffset = uint32(bytes4(data[60:64]));
        signedAmountIn = uint256(bytes32(data[64:96]));

        // The remaining bytes are the opaque Native Router calldata
        payload = data[_FIXED_HEADER_LENGTH:];
    }

    function _validateAmountInOffset(
        bytes memory payload,
        uint32 amountInOffset
    ) private pure {
        // Native's offset is relative to the complete Router calldata. The first
        // ABI word begins after the 4-byte selector, and every argument word is
        // 32-byte aligned from there.
        if (
            amountInOffset < 4 || (amountInOffset - 4) % 32 != 0
                || payload.length < 32 || amountInOffset > payload.length - 32
        ) {
            revert NativeExecutor__InvalidAmountInOffset();
        }
    }

    function _setActualSellerAmount(
        bytes memory payload,
        uint32 amountInOffset,
        uint256 amountIn
    ) private pure {
        assembly ("memory-safe") {
            mstore(add(add(payload, 0x20), amountInOffset), amountIn)
        }
    }

    function _isValidTarget(address target) private view returns (bool) {
        return target == nativeRouterV4;
    }

    function getTransferData(bytes calldata data)
        external
        view
        returns (
            TransferManager.TransferType transferType,
            address receiver,
            address tokenIn,
            address tokenOut,
            bool outputToRouter
        )
    {
        address target;
        (tokenIn, tokenOut, target,,,) = _decodeData(data);

        if (!_isValidTarget(target)) {
            revert NativeExecutor__InvalidTarget();
        }

        if (tokenIn == ETH_ADDRESS) {
            transferType = TransferManager.TransferType.TransferNativeInExecutor;
            // When transferring ETH in the executor, receiver doesn't need to be set
            // because the ETH stays in the Dispatcher until the executor is called with msg.value
            receiver = address(0);
        } else {
            transferType = TransferManager.TransferType.ProtocolWillDebit;
            receiver = target;
        }

        outputToRouter = true;
    }
}
